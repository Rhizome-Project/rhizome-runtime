package sqlite

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func requireApproxFloat(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.0001 {
		t.Fatalf("got %.6f, want %.6f", got, want)
	}
}

func TestBundleUtilityNormalizedSignalClampsToUnitInterval(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input float64
		want  float64
	}{
		{name: "negative", input: -0.4, want: 0},
		{name: "in range", input: 0.65, want: 0.65},
		{name: "above one", input: 1.4, want: 1},
		{name: "nan", input: math.NaN(), want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bundleUtilityNormalizedSignal(tc.input)
			if got != tc.want {
				t.Fatalf("bundleUtilityNormalizedSignal(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestCalculateBundleUtilityRewardsUnlockAndCoverage(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:      0.05,
		UnlockScore:    0.6,
		CoverageScore:  0.8,
		AlphaUnlock:    0.25,
		AlphaCoverage:  0.35,
		MergeThreshold: 0.1,
	})

	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
	if decision.SupportClass != "coverage_dominant" {
		t.Fatalf("support class = %s, want coverage_dominant", decision.SupportClass)
	}
	if decision.MergeClass != "ungated_coverage_support" {
		t.Fatalf("merge class = %s, want ungated_coverage_support", decision.MergeClass)
	}
	requireApproxFloat(t, decision.BenefitScore, 0.43)
	requireApproxFloat(t, decision.UtilityScore, 0.48)
	if !strings.Contains(decision.Reasoning, "Unlock: 0.15") || !strings.Contains(decision.Reasoning, "Coverage: 0.28") {
		t.Fatalf("reasoning did not surface unlock/coverage bonuses: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityClampsUnlockAndCoverageSignals(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:      0.2,
		UnlockScore:    -0.8,
		CoverageScore:  2.4,
		AlphaUnlock:    0.3,
		AlphaCoverage:  0.4,
		MergeThreshold: 0.55,
	})

	requireApproxFloat(t, decision.BenefitScore, 0.4)
	requireApproxFloat(t, decision.UtilityScore, 0.6)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
}

func TestCalculateBundleUtilityPenalizesRedundancyAndSoftConflict(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:         0.9,
		UnlockScore:       0.4,
		CoverageScore:     0.4,
		RedundancyScore:   0.5,
		SoftConflictScore: 0.6,
		AlphaUnlock:       0.2,
		AlphaCoverage:     0.2,
		AlphaRedundancy:   0.3,
		AlphaSoftConflict: 0.25,
		MergeThreshold:    0.75,
	})

	requireApproxFloat(t, decision.BenefitScore, 0.16)
	requireApproxFloat(t, decision.PenaltyScore, 0.3)
	requireApproxFloat(t, decision.UtilityScore, 0.76)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "Redundancy: 0.15") || !strings.Contains(decision.Reasoning, "SoftConflict: 0.15") {
		t.Fatalf("reasoning did not surface redundancy/soft-conflict penalties: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityClampsRedundancyAndSoftConflictSignals(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:         1.0,
		RedundancyScore:   2.2,
		SoftConflictScore: -0.4,
		AlphaRedundancy:   0.5,
		AlphaSoftConflict: 0.4,
		MergeThreshold:    0.45,
	})

	requireApproxFloat(t, decision.PenaltyScore, 0.5)
	requireApproxFloat(t, decision.UtilityScore, 0.5)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
}

func TestCalculateBundleUtilityPenalizesLeaseRisk(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:      0.85,
		UnlockScore:    0.5,
		CoverageScore:  0.4,
		LeaseRiskScore: 0.5,
		AlphaUnlock:    0.2,
		AlphaCoverage:  0.2,
		AlphaLeaseRisk: 0.3,
		MergeThreshold: 0.75,
	})

	requireApproxFloat(t, decision.BenefitScore, 0.18)
	requireApproxFloat(t, decision.PenaltyScore, 0.15)
	requireApproxFloat(t, decision.UtilityScore, 0.88)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "LeaseRisk: 0.15") {
		t.Fatalf("reasoning did not surface lease-risk penalty: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityClampsLeaseRiskSignal(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:      1.0,
		LeaseRiskScore: 2.3,
		AlphaLeaseRisk: 0.45,
		MergeThreshold: 0.5,
	})

	requireApproxFloat(t, decision.PenaltyScore, 0.45)
	requireApproxFloat(t, decision.UtilityScore, 0.55)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
}

func TestBundleUtilityMergeGateRequiresBaseUnlockOrCoverageTrigger(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityMergeGate(BundleUtilityParams{
		BaseValue:              0.08,
		UnlockScore:            0.2,
		CoverageScore:          0.2,
		MergeBaseThreshold:     0.2,
		MergeUnlockThreshold:   0.4,
		MergeCoverageThreshold: 0.4,
	})
	if ok {
		t.Fatal("expected merge gate to fail")
	}
	if reason != "unmet" {
		t.Fatalf("gate reason = %s, want unmet", reason)
	}
}

func TestBundleUtilityMergeGateCanBeSatisfiedByCoverage(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityMergeGate(BundleUtilityParams{
		BaseValue:              0.08,
		UnlockScore:            0.2,
		CoverageScore:          0.75,
		MergeBaseThreshold:     0.2,
		MergeUnlockThreshold:   0.4,
		MergeCoverageThreshold: 0.6,
	})
	if !ok {
		t.Fatal("expected merge gate to pass")
	}
	if reason != "coverage" {
		t.Fatalf("gate reason = %s, want coverage", reason)
	}
}

func TestCalculateBundleUtilityForksWhenGateIsUnmetDespitePositiveUtility(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:              0.1,
		UnlockScore:            0.15,
		CoverageScore:          0.2,
		AlphaUnlock:            0.3,
		AlphaCoverage:          0.3,
		MergeThreshold:         0.1,
		MergeBaseThreshold:     0.25,
		MergeUnlockThreshold:   0.4,
		MergeCoverageThreshold: 0.5,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.205)
	if decision.GateSatisfied {
		t.Fatal("expected gate to remain unsatisfied")
	}
	if decision.GateReason != "unmet" {
		t.Fatalf("gate reason = %s, want unmet", decision.GateReason)
	}
	if decision.DecisionReason != "gate_unmet" {
		t.Fatalf("decision reason = %s, want gate_unmet", decision.DecisionReason)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "Gate: unmet") {
		t.Fatalf("reasoning did not surface unmet gate: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityMergesWhenCoverageGateIsSatisfied(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:              0.05,
		UnlockScore:            0.1,
		CoverageScore:          0.8,
		AlphaUnlock:            0.2,
		AlphaCoverage:          0.3,
		MergeThreshold:         0.2,
		MergeBaseThreshold:     0.2,
		MergeUnlockThreshold:   0.4,
		MergeCoverageThreshold: 0.75,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.31)
	if !decision.GateSatisfied {
		t.Fatal("expected gate to be satisfied")
	}
	if decision.GateReason != "coverage" {
		t.Fatalf("gate reason = %s, want coverage", decision.GateReason)
	}
	if decision.DecisionReason != "merge_allowed" {
		t.Fatalf("decision reason = %s, want merge_allowed", decision.DecisionReason)
	}
	if decision.MergeClass != "coverage_gate" {
		t.Fatalf("merge class = %s, want coverage_gate", decision.MergeClass)
	}
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "MergeClass: coverage_gate") {
		t.Fatalf("reasoning did not surface coverage merge class: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityForksOnHardConflictDespitePositiveUtility(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:              0.4,
		UnlockScore:            0.7,
		CoverageScore:          0.7,
		AlphaUnlock:            0.2,
		AlphaCoverage:          0.2,
		MergeThreshold:         0.2,
		MergeBaseThreshold:     0.2,
		MergeUnlockThreshold:   0.4,
		MergeCoverageThreshold: 0.4,
		HardConflict:           true,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.68)
	if !decision.GateSatisfied {
		t.Fatal("expected merge gate to be satisfied before hard-conflict rejection")
	}
	if !decision.HardConflict {
		t.Fatal("expected hard conflict to be surfaced")
	}
	if decision.DecisionReason != "hard_conflict" {
		t.Fatalf("decision reason = %s, want hard_conflict", decision.DecisionReason)
	}
	if decision.MergeClass != "none" {
		t.Fatalf("merge class = %s, want none", decision.MergeClass)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "HardConflict: true") {
		t.Fatalf("reasoning did not surface hard conflict: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityForksWhenPenaltiesOutweighUnlockAndCoverage(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:            0.1,
		UnlockScore:          1,
		CoverageScore:        1,
		RedundancyScore:      1,
		SoftConflictScore:    1,
		LeaseRiskScore:       1,
		AlphaUnlock:          0.2,
		AlphaCoverage:        0.2,
		AlphaRedundancy:      0.25,
		AlphaSoftConflict:    0.25,
		AlphaLeaseRisk:       0.25,
		ContradictEdgesCount: 1,
		VerifierFailsCount:   1,
		Lambda1:              0.5,
		Lambda2:              1.0,
		MergeThreshold:       0.05,
	})

	requireApproxFloat(t, decision.BenefitScore, 0.4)
	requireApproxFloat(t, decision.PenaltyScore, 2.25)
	requireApproxFloat(t, decision.UtilityScore, -1.75)
	if decision.DecisionReason != "utility_below_threshold" {
		t.Fatalf("decision reason = %s, want utility_below_threshold", decision.DecisionReason)
	}
	if decision.DecisionDetail != "verifier_fails" {
		t.Fatalf("decision detail = %s, want verifier_fails", decision.DecisionDetail)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "Detail: verifier_fails") {
		t.Fatalf("reasoning did not surface utility-failure detail: %s", decision.Reasoning)
	}
}

func TestBundleUtilityAdmissionRiskRejectsHighLeaseRisk(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityAdmissionRisk(BundleUtilityParams{
		RedundancyScore:                0.2,
		SoftConflictScore:              0.3,
		LeaseRiskScore:                 0.82,
		AdmissionRedundancyThreshold:   0.9,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionCombinedRiskThreshold: 0.9,
	})
	if ok {
		t.Fatal("expected admission to fail")
	}
	if reason != "lease_risk" {
		t.Fatalf("admission reason = %s, want lease_risk", reason)
	}
}

func TestBundleUtilityAdmissionRiskRejectsCombinedRisk(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityAdmissionRisk(BundleUtilityParams{
		RedundancyScore:                0.7,
		SoftConflictScore:              0.7,
		LeaseRiskScore:                 0.6,
		AdmissionRedundancyThreshold:   0.9,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.6,
	})
	if ok {
		t.Fatal("expected admission to fail")
	}
	if reason != "combined_risk" {
		t.Fatalf("admission reason = %s, want combined_risk", reason)
	}
}

func TestCalculateBundleUtilityForksWhenAdmissionRiskIsTooHigh(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		LeaseRiskScore:                 0.82,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaLeaseRisk:                 0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionRedundancyThreshold:   0.9,
		AdmissionCombinedRiskThreshold: 0.9,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.599)
	if !decision.GateSatisfied {
		t.Fatal("expected merge gate to be satisfied before admission rejection")
	}
	if decision.AdmissionSatisfied {
		t.Fatal("expected admission to fail")
	}
	if decision.AdmissionReason != "lease_risk" {
		t.Fatalf("admission reason = %s, want lease_risk", decision.AdmissionReason)
	}
	if decision.AdmissionClass != "rejected_lease_risk" {
		t.Fatalf("admission class = %s, want rejected_lease_risk", decision.AdmissionClass)
	}
	if decision.ConflictSafeClass != "fork_required" {
		t.Fatalf("conflict-safe class = %s, want fork_required", decision.ConflictSafeClass)
	}
	if decision.NextAction != "hard_fork" {
		t.Fatalf("next action = %s, want hard_fork", decision.NextAction)
	}
	if decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to fail")
	}
	if decision.RebaseReason != "not_overlap_admission" {
		t.Fatalf("rebase reason = %s, want not_overlap_admission", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "none" {
		t.Fatalf("rebase plan class = %s, want none", decision.RebasePlanClass)
	}
	if decision.DecisionReason != "admission_lease_risk" {
		t.Fatalf("decision reason = %s, want admission_lease_risk", decision.DecisionReason)
	}
	if decision.DecisionDetail != "lease_risk" {
		t.Fatalf("decision detail = %s, want lease_risk", decision.DecisionDetail)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "Admission: lease_risk") {
		t.Fatalf("reasoning did not surface admission failure: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityForksWithRebaseCandidateConflictSafeClass(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.595)
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if decision.DecisionReason != "admission_redundancy" {
		t.Fatalf("decision reason = %s, want admission_redundancy", decision.DecisionReason)
	}
	if decision.ConflictSafeClass != "rebase_candidate" {
		t.Fatalf("conflict-safe class = %s, want rebase_candidate", decision.ConflictSafeClass)
	}
	if decision.NextAction != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", decision.NextAction)
	}
	if !decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to pass")
	}
	if decision.RebaseReason != "clear" {
		t.Fatalf("rebase reason = %s, want clear", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "trim_redundancy" {
		t.Fatalf("rebase plan class = %s, want trim_redundancy", decision.RebasePlanClass)
	}
	if !strings.Contains(decision.Reasoning, "ConflictSafeClass: rebase_candidate") {
		t.Fatalf("reasoning did not surface rebase-candidate class: %s", decision.Reasoning)
	}
}

func TestCalculateBundleUtilityOverlapForkRequiresHardForkWhenRebaseAdmissionFails(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		LeaseRiskScore:                 0.5,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.595)
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if decision.DecisionReason != "admission_redundancy" {
		t.Fatalf("decision reason = %s, want admission_redundancy", decision.DecisionReason)
	}
	if decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to fail")
	}
	if decision.RebaseReason != "lease_risk" {
		t.Fatalf("rebase reason = %s, want lease_risk", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "none" {
		t.Fatalf("rebase plan class = %s, want none", decision.RebasePlanClass)
	}
	if decision.ConflictSafeClass != "fork_required" {
		t.Fatalf("conflict-safe class = %s, want fork_required", decision.ConflictSafeClass)
	}
	if decision.NextAction != "hard_fork" {
		t.Fatalf("next action = %s, want hard_fork", decision.NextAction)
	}
}

func TestCalculateBundleUtilityMergesWhenAdmissionRiskIsClear(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.28,
		UnlockScore:                    0.55,
		CoverageScore:                  0.45,
		RedundancyScore:                0.2,
		SoftConflictScore:              0.2,
		LeaseRiskScore:                 0.2,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		AlphaSoftConflict:              0.05,
		AlphaLeaseRisk:                 0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionCombinedRiskThreshold: 0.75,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.45)
	if !decision.GateSatisfied {
		t.Fatal("expected gate to be satisfied")
	}
	if !decision.AdmissionSatisfied {
		t.Fatal("expected admission to be satisfied")
	}
	if decision.AdmissionReason != "clear" {
		t.Fatalf("admission reason = %s, want clear", decision.AdmissionReason)
	}
	if decision.AdmissionClass != "clear" {
		t.Fatalf("admission class = %s, want clear", decision.AdmissionClass)
	}
	if decision.SupportClass != "balanced" {
		t.Fatalf("support class = %s, want balanced", decision.SupportClass)
	}
	if decision.DecisionReason != "merge_allowed" {
		t.Fatalf("decision reason = %s, want merge_allowed", decision.DecisionReason)
	}
	if decision.DecisionDetail != "merge_allowed" {
		t.Fatalf("decision detail = %s, want merge_allowed", decision.DecisionDetail)
	}
	if decision.ConflictSafeClass != "merge_direct" {
		t.Fatalf("conflict-safe class = %s, want merge_direct", decision.ConflictSafeClass)
	}
	if decision.NextAction != "merge_bundle" {
		t.Fatalf("next action = %s, want merge_bundle", decision.NextAction)
	}
	if decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to be unnecessary on merge path")
	}
	if decision.RebaseReason != "not_needed" {
		t.Fatalf("rebase reason = %s, want not_needed", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "none" {
		t.Fatalf("rebase plan class = %s, want none", decision.RebasePlanClass)
	}
	if decision.MergeClass != "base_gate" {
		t.Fatalf("merge class = %s, want base_gate", decision.MergeClass)
	}
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
}

func TestBundleUtilityMergeClassUsesCoverageGate(t *testing.T) {
	t.Parallel()

	class := bundleUtilityMergeClass(BundleUtilityParams{BaseValue: 0.1}, "coverage", 0.08, 0.16)
	if class != "coverage_gate" {
		t.Fatalf("merge class = %s, want coverage_gate", class)
	}
}

func TestBundleUtilityMergeClassUsesUngatedCoverageSupport(t *testing.T) {
	t.Parallel()

	class := bundleUtilityMergeClass(BundleUtilityParams{BaseValue: 0.05}, "disabled", 0.08, 0.16)
	if class != "ungated_coverage_support" {
		t.Fatalf("merge class = %s, want ungated_coverage_support", class)
	}
}

func TestBundleUtilityAdmissionClassReturnsGuardedLeaseRisk(t *testing.T) {
	t.Parallel()

	class := bundleUtilityAdmissionClass(BundleUtilityParams{
		LeaseRiskScore:                 0.62,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionCombinedRiskThreshold: 0.9,
	}, true, "clear")
	if class != "guarded_lease_risk" {
		t.Fatalf("admission class = %s, want guarded_lease_risk", class)
	}
}

func TestBundleUtilityAdmissionClassReturnsBuffered(t *testing.T) {
	t.Parallel()

	class := bundleUtilityAdmissionClass(BundleUtilityParams{
		RedundancyScore:                0.3,
		SoftConflictScore:              0.2,
		LeaseRiskScore:                 0.2,
		AdmissionRedundancyThreshold:   0.5,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionCombinedRiskThreshold: 0.9,
	}, true, "clear")
	if class != "buffered" {
		t.Fatalf("admission class = %s, want buffered", class)
	}
}

func TestCalculateBundleUtilityMergesWithGuardedAdmissionClass(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.3,
		UnlockScore:                    0.4,
		CoverageScore:                  0.4,
		LeaseRiskScore:                 0.62,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaLeaseRisk:                 0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionCombinedRiskThreshold: 0.9,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.429)
	if decision.Decision != "MERGE" {
		t.Fatalf("decision = %s, want MERGE", decision.Decision)
	}
	if decision.AdmissionSatisfied != true {
		t.Fatal("expected admission to stay satisfied")
	}
	if decision.AdmissionClass != "guarded_lease_risk" {
		t.Fatalf("admission class = %s, want guarded_lease_risk", decision.AdmissionClass)
	}
	if decision.ConflictSafeClass != "merge_guarded" {
		t.Fatalf("conflict-safe class = %s, want merge_guarded", decision.ConflictSafeClass)
	}
	if decision.NextAction != "merge_bundle" {
		t.Fatalf("next action = %s, want merge_bundle", decision.NextAction)
	}
	if decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to be unnecessary on guarded merge path")
	}
	if decision.RebaseReason != "not_needed" {
		t.Fatalf("rebase reason = %s, want not_needed", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "none" {
		t.Fatalf("rebase plan class = %s, want none", decision.RebasePlanClass)
	}
	if decision.SupportClass != "balanced" {
		t.Fatalf("support class = %s, want balanced", decision.SupportClass)
	}
	if !strings.Contains(decision.Reasoning, "AdmissionClass: guarded_lease_risk") {
		t.Fatalf("reasoning did not surface guarded admission class: %s", decision.Reasoning)
	}
}

func TestBundleUtilitySupportClassReturnsCoverageDominant(t *testing.T) {
	t.Parallel()

	class := bundleUtilitySupportClass(0.10, 0.18)
	if class != "coverage_dominant" {
		t.Fatalf("support class = %s, want coverage_dominant", class)
	}
}

func TestBundleUtilitySupportClassReturnsBalanced(t *testing.T) {
	t.Parallel()

	class := bundleUtilitySupportClass(0.11, 0.09)
	if class != "balanced" {
		t.Fatalf("support class = %s, want balanced", class)
	}
}

func TestBundleUtilitySupportClassReturnsBaseOnly(t *testing.T) {
	t.Parallel()

	class := bundleUtilitySupportClass(0, 0)
	if class != "base_only" {
		t.Fatalf("support class = %s, want base_only", class)
	}
}

func TestBundleUtilityConflictSafeClassReturnsMergeDirect(t *testing.T) {
	t.Parallel()

	class := bundleUtilityConflictSafeClass("MERGE", "merge_allowed", "clear", false)
	if class != "merge_direct" {
		t.Fatalf("conflict-safe class = %s, want merge_direct", class)
	}
}

func TestBundleUtilityConflictSafeClassReturnsMergeGuarded(t *testing.T) {
	t.Parallel()

	class := bundleUtilityConflictSafeClass("MERGE", "merge_allowed", "guarded_lease_risk", false)
	if class != "merge_guarded" {
		t.Fatalf("conflict-safe class = %s, want merge_guarded", class)
	}
}

func TestBundleUtilityConflictSafeClassReturnsRebaseCandidate(t *testing.T) {
	t.Parallel()

	class := bundleUtilityConflictSafeClass("FORK", "admission_combined_risk", "rejected_combined_risk", true)
	if class != "rebase_candidate" {
		t.Fatalf("conflict-safe class = %s, want rebase_candidate", class)
	}
}

func TestBundleUtilityConflictSafeClassReturnsForkRequired(t *testing.T) {
	t.Parallel()

	class := bundleUtilityConflictSafeClass("FORK", "utility_below_threshold", "clear", false)
	if class != "fork_required" {
		t.Fatalf("conflict-safe class = %s, want fork_required", class)
	}
}

func TestBundleUtilityRebasePlanClassReturnsTrimRedundancy(t *testing.T) {
	t.Parallel()

	class := bundleUtilityRebasePlanClass("admission_redundancy", true)
	if class != "trim_redundancy" {
		t.Fatalf("rebase plan class = %s, want trim_redundancy", class)
	}
}

func TestBundleUtilityRebasePlanClassReturnsSplitOverlapCluster(t *testing.T) {
	t.Parallel()

	class := bundleUtilityRebasePlanClass("admission_combined_risk", true)
	if class != "split_overlap_cluster" {
		t.Fatalf("rebase plan class = %s, want split_overlap_cluster", class)
	}
}

func TestBundleUtilityRebasePlanClassReturnsNoneWhenRebaseIsBlocked(t *testing.T) {
	t.Parallel()

	class := bundleUtilityRebasePlanClass("admission_soft_conflict", false)
	if class != "none" {
		t.Fatalf("rebase plan class = %s, want none", class)
	}
}

func TestBundleUtilityNextActionReturnsAttemptRebase(t *testing.T) {
	t.Parallel()

	action := bundleUtilityNextAction("FORK", true)
	if action != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", action)
	}
}

func TestBundleUtilityNextActionReturnsHardFork(t *testing.T) {
	t.Parallel()

	action := bundleUtilityNextAction("FORK", false)
	if action != "hard_fork" {
		t.Fatalf("next action = %s, want hard_fork", action)
	}
}

func TestBundleUtilityNextActionReturnsMergeBundle(t *testing.T) {
	t.Parallel()

	action := bundleUtilityNextAction("MERGE", false)
	if action != "merge_bundle" {
		t.Fatalf("next action = %s, want merge_bundle", action)
	}
}

func TestBundleUtilityForkConstraintRefsIncludeNextActionAndRebasePlan(t *testing.T) {
	t.Parallel()

	refs := bundleUtilityForkConstraintRefs(BundleDecision{
		DecisionReason:    "admission_redundancy",
		DecisionDetail:    "redundancy",
		ConflictSafeClass: "rebase_candidate",
		NextAction:        "attempt_rebase",
		RebaseReason:      "clear",
		RebasePlanClass:   "trim_redundancy",
	})
	joined := strings.Join(refs, ",")
	if !strings.Contains(joined, "bundle_decision:admission_redundancy") {
		t.Fatalf("constraint refs missing decision: %v", refs)
	}
	if !strings.Contains(joined, "next_action:attempt_rebase") {
		t.Fatalf("constraint refs missing next action: %v", refs)
	}
	if !strings.Contains(joined, "rebase_plan:trim_redundancy") {
		t.Fatalf("constraint refs missing rebase plan: %v", refs)
	}
}

func TestBundleUtilityForkEvidenceRecordsSurfaceArtifactAndDecisionEnvelope(t *testing.T) {
	t.Parallel()

	evidence := bundleUtilityForkEvidenceRecords(
		"tens-1",
		"ws-1",
		[]string{"patch-ref-r10"},
		[]string{"next_action:attempt_rebase", "rebase_plan:trim_redundancy"},
		"2026-04-02T00:00:00Z",
	)
	if len(evidence) != 3 {
		t.Fatalf("evidence count = %d, want 3", len(evidence))
	}

	joined := []string{}
	for _, item := range evidence {
		joined = append(joined, item.EvidenceKind+":"+item.EvidenceRef)
	}
	all := strings.Join(joined, ",")
	if !strings.Contains(all, "bundle_artifact:patch-ref-r10") {
		t.Fatalf("evidence missing artifact envelope: %v", joined)
	}
	if !strings.Contains(all, "bundle_decision:next_action:attempt_rebase") {
		t.Fatalf("evidence missing next action envelope: %v", joined)
	}
}

func TestBundleUtilityRebaseFollowupConstraintRefsIncludeParentAndPlan(t *testing.T) {
	t.Parallel()

	refs := bundleUtilityRebaseFollowupConstraintRefs("tens-parent", BundleDecision{
		DecisionReason:    "admission_redundancy",
		DecisionDetail:    "redundancy",
		ConflictSafeClass: "rebase_candidate",
		NextAction:        "attempt_rebase",
		RebaseReason:      "clear",
		RebasePlanClass:   "trim_redundancy",
	})
	joined := strings.Join(refs, ",")
	if !strings.Contains(joined, "parent_tension:tens-parent") {
		t.Fatalf("constraint refs missing parent tension: %v", refs)
	}
	if !strings.Contains(joined, "rebase_plan:trim_redundancy") {
		t.Fatalf("constraint refs missing rebase plan: %v", refs)
	}
}

func TestBundleUtilityRebaseFollowupEvidenceRecordsSurfaceParentAndArtifact(t *testing.T) {
	t.Parallel()

	evidence := bundleUtilityRebaseFollowupEvidenceRecords(
		"tens-followup",
		"ws-1",
		"tens-parent",
		"rtev-parent",
		[]string{"patch-ref-r10"},
		[]string{"next_action:attempt_rebase", "rebase_plan:trim_redundancy"},
		"2026-04-02T00:00:00Z",
	)
	if len(evidence) != 5 {
		t.Fatalf("evidence count = %d, want 5", len(evidence))
	}

	joined := []string{}
	for _, item := range evidence {
		joined = append(joined, item.EvidenceKind+":"+item.EvidenceRef)
	}
	all := strings.Join(joined, ",")
	if !strings.Contains(all, "parent_tension:tens-parent") {
		t.Fatalf("evidence missing parent tension envelope: %v", joined)
	}
	if !strings.Contains(all, "runtime_event:rtev-parent") {
		t.Fatalf("evidence missing parent runtime event envelope: %v", joined)
	}
	if !strings.Contains(all, "bundle_artifact:patch-ref-r10") {
		t.Fatalf("evidence missing artifact envelope: %v", joined)
	}
}

func TestBundleUtilityRebaseFollowupQueueKeyUsesTensionID(t *testing.T) {
	t.Parallel()

	key := bundleUtilityRebaseFollowupQueueKey("tens-followup")
	if key != "tension_rebase_followup:tens-followup" {
		t.Fatalf("queue key = %s, want tension_rebase_followup:tens-followup", key)
	}
}

func TestBundleUtilityRebaseFollowupQueueInputSurfacesRepairEnvelope(t *testing.T) {
	t.Parallel()

	input := bundleUtilityRebaseFollowupQueueInput(
		"ws-1",
		"coal-1",
		"tens-parent",
		"tens-followup",
		"task-r10",
		"agent-r10",
		"tension_rebase_followup:tens-followup",
		"patch-ref-r10",
		[]string{"task-r10"},
		BundleDecision{
			NextAction:        "attempt_rebase",
			RebasePlanClass:   "trim_redundancy",
			RebaseReason:      "clear",
			ConflictSafeClass: "rebase_candidate",
			DecisionReason:    "admission_redundancy",
			DecisionDetail:    "redundancy",
		},
	)
	if input.QueueKey != "tension_rebase_followup:tens-followup" {
		t.Fatalf("queue key = %s", input.QueueKey)
	}
	if input.AgentID != "agent-r10" {
		t.Fatalf("agent id = %s, want %s", input.AgentID, "agent-r10")
	}
	if input.SourceKind != "tension" || input.SourceID != "tens-followup" {
		t.Fatalf("unexpected source envelope %+v", input)
	}
	if input.TaskID != "task-r10" {
		t.Fatalf("task id = %s, want task-r10", input.TaskID)
	}
	if !input.KeepSessionActive {
		t.Fatal("expected keep_session_active=true")
	}
	if !strings.Contains(input.Details, "Fork candidate tension: tens-parent") {
		t.Fatalf("details did not surface parent tension: %s", input.Details)
	}
	if !strings.Contains(input.PayloadJSON, `"repair_tension_id":"tens-followup"`) {
		t.Fatalf("payload did not surface repair tension id: %s", input.PayloadJSON)
	}
	if !strings.Contains(input.PayloadJSON, `"task_id":"task-r10"`) {
		t.Fatalf("payload did not surface task id: %s", input.PayloadJSON)
	}
}

func TestBundleUtilityRebaseAdmissionReturnsClearForOverlapFork(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityRebaseAdmission(BundleUtilityParams{
		LeaseRiskScore:              0.2,
		AdmissionLeaseRiskThreshold: 0.9,
	}, "FORK", "admission_redundancy")
	if !ok {
		t.Fatal("expected rebase admission to pass")
	}
	if reason != "clear" {
		t.Fatalf("rebase reason = %s, want clear", reason)
	}
}

func TestBundleUtilityRebaseAdmissionRejectsLeaseRisk(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityRebaseAdmission(BundleUtilityParams{
		LeaseRiskScore:              0.5,
		AdmissionLeaseRiskThreshold: 0.9,
	}, "FORK", "admission_soft_conflict")
	if ok {
		t.Fatal("expected rebase admission to fail")
	}
	if reason != "lease_risk" {
		t.Fatalf("rebase reason = %s, want lease_risk", reason)
	}
}

func TestBundleUtilityRebaseAdmissionRejectsVerifierFails(t *testing.T) {
	t.Parallel()

	ok, reason := bundleUtilityRebaseAdmission(BundleUtilityParams{
		VerifierFailsCount:          1,
		LeaseRiskScore:              0.1,
		AdmissionLeaseRiskThreshold: 0.9,
	}, "FORK", "admission_combined_risk")
	if ok {
		t.Fatal("expected rebase admission to fail")
	}
	if reason != "verifier_fails" {
		t.Fatalf("rebase reason = %s, want verifier_fails", reason)
	}
}

func TestBundleUtilityThresholdFailureDetailReturnsBenefitGap(t *testing.T) {
	t.Parallel()

	detail := bundleUtilityThresholdFailureDetail(
		BundleUtilityParams{MergeThreshold: 0.3},
		0.25,
		0.05,
		0.04,
		0.03,
		0.02,
		0.01,
	)
	if detail != "benefit_gap" {
		t.Fatalf("detail = %s, want benefit_gap", detail)
	}
}

func TestBundleUtilityThresholdFailureDetailPrefersLeaseRiskPenalty(t *testing.T) {
	t.Parallel()

	detail := bundleUtilityThresholdFailureDetail(
		BundleUtilityParams{MergeThreshold: 0.2},
		0.45,
		0.08,
		0.09,
		0.16,
		0.05,
		0.04,
	)
	if detail != "lease_risk" {
		t.Fatalf("detail = %s, want lease_risk", detail)
	}
}

func TestCalculateBundleUtilityForksOnBenefitGapBelowThreshold(t *testing.T) {
	t.Parallel()

	decision := CalculateBundleUtility(BundleUtilityParams{
		BaseValue:                      0.08,
		UnlockScore:                    0.2,
		CoverageScore:                  0.15,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.05,
		MergeUnlockThreshold:           0.15,
		MergeCoverageThreshold:         0.1,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionLeaseRiskThreshold:    0.7,
		AdmissionCombinedRiskThreshold: 0.75,
	})

	requireApproxFloat(t, decision.UtilityScore, 0.15)
	if !decision.GateSatisfied {
		t.Fatal("expected gate to be satisfied")
	}
	if !decision.AdmissionSatisfied {
		t.Fatal("expected admission to be satisfied")
	}
	if decision.DecisionReason != "utility_below_threshold" {
		t.Fatalf("decision reason = %s, want utility_below_threshold", decision.DecisionReason)
	}
	if decision.DecisionDetail != "benefit_gap" {
		t.Fatalf("decision detail = %s, want benefit_gap", decision.DecisionDetail)
	}
	if decision.Decision != "FORK" {
		t.Fatalf("decision = %s, want FORK", decision.Decision)
	}
	if !strings.Contains(decision.Reasoning, "Detail: benefit_gap") {
		t.Fatalf("reasoning did not surface benefit-gap detail: %s", decision.Reasoning)
	}
}

func TestEvaluateCoalitionBundleForkCandidateSurfacesConflictSafeClass(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-bundle-conflict-safe"

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, "coalition-r10-1", BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-r10")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.ConflictSafeClass != "rebase_candidate" {
		t.Fatalf("conflict-safe class = %s, want rebase_candidate", decision.ConflictSafeClass)
	}
	if decision.NextAction != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", decision.NextAction)
	}
	if !decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to pass")
	}
	if decision.RebaseReason != "clear" {
		t.Fatalf("rebase reason = %s, want clear", decision.RebaseReason)
	}
	if decision.RebasePlanClass != "trim_redundancy" {
		t.Fatalf("rebase plan class = %s, want trim_redundancy", decision.RebasePlanClass)
	}

	var forkTensionID, title, summary, anchorKind, anchorRef, artifactRefsJSON, constraintRefsJSON string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id, title, summary, anchor_kind, anchor_ref, artifact_refs_json, constraint_refs_json
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'fork_candidate'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&forkTensionID, &title, &summary, &anchorKind, &anchorRef, &artifactRefsJSON, &constraintRefsJSON); err != nil {
		t.Fatalf("query fork candidate tension: %v", err)
	}
	if !strings.Contains(title, "attempt_rebase/rebase_candidate") {
		t.Fatalf("fork candidate title did not surface conflict-safe class: %s", title)
	}
	if !strings.Contains(summary, "Conflict-safe branch: rebase_candidate") {
		t.Fatalf("fork candidate summary did not surface conflict-safe class: %s", summary)
	}
	if !strings.Contains(summary, "Next action: attempt_rebase") {
		t.Fatalf("fork candidate summary did not surface next action: %s", summary)
	}
	if !strings.Contains(summary, "Rebase admission: clear") {
		t.Fatalf("fork candidate summary did not surface rebase admission: %s", summary)
	}
	if !strings.Contains(summary, "Rebase plan: trim_redundancy") {
		t.Fatalf("fork candidate summary did not surface rebase plan: %s", summary)
	}
	if anchorKind != "coalition_id" {
		t.Fatalf("anchor kind = %s, want coalition_id", anchorKind)
	}
	if anchorRef != "coalition-r10-1" {
		t.Fatalf("anchor ref = %s, want coalition-r10-1", anchorRef)
	}
	if !strings.Contains(artifactRefsJSON, "patch-ref-r10") {
		t.Fatalf("fork candidate artifact refs did not surface patch ref: %s", artifactRefsJSON)
	}
	if !strings.Contains(constraintRefsJSON, "next_action:attempt_rebase") {
		t.Fatalf("fork candidate constraint refs did not surface next action: %s", constraintRefsJSON)
	}
	if !strings.Contains(constraintRefsJSON, "rebase_plan:trim_redundancy") {
		t.Fatalf("fork candidate constraint refs did not surface rebase plan: %s", constraintRefsJSON)
	}

	var parentEventID, payload string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT event_id, payload_json
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = 'coalition_fork_generated'
		 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&parentEventID, &payload); err != nil {
		t.Fatalf("query fork runtime event: %v", err)
	}
	if !strings.Contains(payload, `"conflict_safe_class":"rebase_candidate"`) {
		t.Fatalf("runtime event payload did not surface conflict-safe class: %s", payload)
	}
	if !strings.Contains(payload, `"decision_reason":"admission_redundancy"`) {
		t.Fatalf("runtime event payload did not surface decision reason: %s", payload)
	}
	if !strings.Contains(payload, `"decision_detail":"redundancy"`) {
		t.Fatalf("runtime event payload did not surface decision detail: %s", payload)
	}
	if !strings.Contains(payload, `"rebase_admissible":true`) {
		t.Fatalf("runtime event payload did not surface rebase admissibility: %s", payload)
	}
	if !strings.Contains(payload, `"rebase_reason":"clear"`) {
		t.Fatalf("runtime event payload did not surface rebase reason: %s", payload)
	}
	if !strings.Contains(payload, `"rebase_plan_class":"trim_redundancy"`) {
		t.Fatalf("runtime event payload did not surface rebase plan class: %s", payload)
	}
	if !strings.Contains(payload, `"next_action":"attempt_rebase"`) {
		t.Fatalf("runtime event payload did not surface next action: %s", payload)
	}
	if !strings.Contains(payload, `"anchor_kind":"coalition_id"`) {
		t.Fatalf("runtime event payload did not surface anchor kind: %s", payload)
	}
	if !strings.Contains(payload, `"constraint_refs":["bundle_decision:admission_redundancy"`) {
		t.Fatalf("runtime event payload did not surface constraint refs envelope: %s", payload)
	}

	var repairID, repairTitle, repairSummary, repairAnchorKind, repairAnchorRef, repairArtifactRefsJSON, repairConstraintRefsJSON string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id, title, summary, anchor_kind, anchor_ref, artifact_refs_json, constraint_refs_json
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&repairID, &repairTitle, &repairSummary, &repairAnchorKind, &repairAnchorRef, &repairArtifactRefsJSON, &repairConstraintRefsJSON); err != nil {
		t.Fatalf("query repair follow-up tension: %v", err)
	}
	if !strings.Contains(repairTitle, "Coalition Rebase (trim_redundancy)") {
		t.Fatalf("repair follow-up title did not surface rebase plan: %s", repairTitle)
	}
	if !strings.Contains(repairSummary, "Bounded overlap rebase follow-up for fork candidate") {
		t.Fatalf("repair follow-up summary did not surface follow-up branch: %s", repairSummary)
	}
	if repairAnchorKind != "coalition_id" {
		t.Fatalf("repair anchor kind = %s, want coalition_id", repairAnchorKind)
	}
	if repairAnchorRef != "coalition-r10-1" {
		t.Fatalf("repair anchor ref = %s, want coalition-r10-1", repairAnchorRef)
	}
	if !strings.Contains(repairArtifactRefsJSON, "patch-ref-r10") {
		t.Fatalf("repair follow-up artifact refs did not surface patch ref: %s", repairArtifactRefsJSON)
	}
	if !strings.Contains(repairConstraintRefsJSON, "parent_tension:"+forkTensionID) {
		t.Fatalf("repair follow-up constraint refs did not surface parent tension: %s", repairConstraintRefsJSON)
	}
	if !strings.Contains(repairConstraintRefsJSON, "rebase_plan:trim_redundancy") {
		t.Fatalf("repair follow-up constraint refs did not surface rebase plan: %s", repairConstraintRefsJSON)
	}
	if !strings.Contains(repairConstraintRefsJSON, "queue:tension_rebase_followup:"+repairID) {
		t.Fatalf("repair follow-up constraint refs did not surface queue key: %s", repairConstraintRefsJSON)
	}

	var dependencyType string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT dependency_type
		  FROM workspace_tension_dependencies
		 WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`,
		workspaceID,
		forkTensionID,
		repairID,
	).Scan(&dependencyType); err != nil {
		t.Fatalf("query fork-to-repair dependency: %v", err)
	}
	if dependencyType != "BLOCKS" {
		t.Fatalf("dependency type = %s, want BLOCKS", dependencyType)
	}

	var repairParentRefsJSON, repairPayload string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT parent_refs_json, payload_json
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = 'tension.emerged' AND entity_id = ?
		 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
		 LIMIT 1`,
		workspaceID,
		repairID,
	).Scan(&repairParentRefsJSON, &repairPayload); err != nil {
		t.Fatalf("query repair follow-up runtime event: %v", err)
	}
	if !strings.Contains(repairParentRefsJSON, parentEventID) {
		t.Fatalf("repair follow-up parent refs did not preserve parent runtime event: %s", repairParentRefsJSON)
	}
	if !strings.Contains(repairPayload, `"parent_tension_id":"`+forkTensionID+`"`) {
		t.Fatalf("repair follow-up payload did not surface parent tension id: %s", repairPayload)
	}
	if !strings.Contains(repairPayload, `"next_action":"attempt_rebase"`) {
		t.Fatalf("repair follow-up payload did not surface next action: %s", repairPayload)
	}
	if !strings.Contains(repairPayload, `"rebase_plan_class":"trim_redundancy"`) {
		t.Fatalf("repair follow-up payload did not surface rebase plan: %s", repairPayload)
	}

	var queueID, queueKey, queueType, queueStatus, queueSourceKind, queueSourceID, queuePayload string
	var keepSessionActive bool
	if err := store.DB().QueryRowContext(ctx, `
		SELECT queue_id, queue_key, queue_type, status, source_kind, source_id, keep_session_active, payload_json
		  FROM operator_queue_items
		 WHERE workspace_id = ? AND source_kind = 'tension' AND source_id = ?
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
		repairID,
	).Scan(&queueID, &queueKey, &queueType, &queueStatus, &queueSourceKind, &queueSourceID, &keepSessionActive, &queuePayload); err != nil {
		t.Fatalf("query repair follow-up operator queue: %v", err)
	}
	if queueKey != "tension_rebase_followup:"+repairID {
		t.Fatalf("queue key = %s, want tension_rebase_followup:%s", queueKey, repairID)
	}
	if queueType != "FOLLOW_UP" || queueStatus != "OPEN" {
		t.Fatalf("unexpected queue type/status %s/%s", queueType, queueStatus)
	}
	if queueSourceKind != "tension" || queueSourceID != repairID {
		t.Fatalf("unexpected queue source envelope %s/%s", queueSourceKind, queueSourceID)
	}
	if !keepSessionActive {
		t.Fatal("expected rebase follow-up queue to keep session active")
	}
	if !strings.Contains(queuePayload, `"fork_tension_id":"`+forkTensionID+`"`) {
		t.Fatalf("queue payload did not surface fork tension id: %s", queuePayload)
	}
	if !strings.Contains(queuePayload, `"repair_tension_id":"`+repairID+`"`) {
		t.Fatalf("queue payload did not surface repair tension id: %s", queuePayload)
	}

	var queueParentRefsJSON, queueEventPayload string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT parent_refs_json, payload_json
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = 'operator_queue.rebase_followup_created' AND entity_id = ?
		 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
		 LIMIT 1`,
		workspaceID,
		queueID,
	).Scan(&queueParentRefsJSON, &queueEventPayload); err != nil {
		t.Fatalf("query rebase follow-up queue runtime event: %v", err)
	}
	if !strings.Contains(queueParentRefsJSON, parentEventID) {
		t.Fatalf("queue runtime event did not preserve fork parent refs: %s", queueParentRefsJSON)
	}
	if !strings.Contains(queueEventPayload, `"repair_tension_id":"`+repairID+`"`) {
		t.Fatalf("queue runtime event payload did not surface repair tension id: %s", queueEventPayload)
	}
	if !strings.Contains(queueEventPayload, `"next_action":"attempt_rebase"`) {
		t.Fatalf("queue runtime event payload did not surface next action: %s", queueEventPayload)
	}
}

func TestEvaluateCoalitionBundleCarriesAuthorityMetadataOnForkAndRebaseEvents(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-bundle-authority-metadata"

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, "coalition-bundle-authority-metadata", BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-bundle-authority-metadata")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.NextAction != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", decision.NextAction)
	}

	var forkTensionID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'fork_candidate'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&forkTensionID); err != nil {
		t.Fatalf("query fork candidate tension id: %v", err)
	}

	var repairID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&repairID); err != nil {
		t.Fatalf("query repair follow-up tension id: %v", err)
	}

	var queueID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT queue_id
		  FROM operator_queue_items
		 WHERE workspace_id = ? AND source_kind = 'tension' AND source_id = ?
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
		repairID,
	).Scan(&queueID); err != nil {
		t.Fatalf("query repair follow-up queue id: %v", err)
	}

	requireRuntimeEventAuthorityMetadata(t, store, ctx, workspaceID, "coalition_fork_generated", forkTensionID, authority)
	requireRuntimeEventAuthorityMetadata(t, store, ctx, workspaceID, "tension.emerged", repairID, authority)
	requireRuntimeEventAuthorityMetadata(t, store, ctx, workspaceID, "operator_queue.created", queueID, authority)
	requireRuntimeEventAuthorityMetadata(t, store, ctx, workspaceID, "operator_queue.rebase_followup_created", queueID, authority)
}

func TestEvaluateCoalitionBundleRebaseFollowupCarriesSourceTaskContext(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-bundle-task-context"
	taskID := "task-bundle-task-context"
	tensionID := "tension:task:" + workspaceID + "/" + taskID

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-bundle-task-context")
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      tensionID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/" + taskID,
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Task-linked bundle source",
		AnchorKind:     "task",
		AnchorRef:      taskID,
		TaskIDs:        []string{taskID},
		AgentIDs:       []string{"agent-a"},
		BaseScore:      70,
		SurfaceScore:   80,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Task-linked bundle test")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, coalition.CoalitionID, BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-task-link")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.NextAction != "attempt_rebase" {
		t.Fatalf("next action = %s, want attempt_rebase", decision.NextAction)
	}

	var forkTaskIDsJSON string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT task_ids_json
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'fork_candidate'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&forkTaskIDsJSON); err != nil {
		t.Fatalf("query fork task ids: %v", err)
	}
	if !strings.Contains(forkTaskIDsJSON, taskID) {
		t.Fatalf("fork candidate task ids did not surface source task: %s", forkTaskIDsJSON)
	}

	var repairID, repairTaskIDsJSON string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT tension_id, task_ids_json
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
	).Scan(&repairID, &repairTaskIDsJSON); err != nil {
		t.Fatalf("query repair task ids: %v", err)
	}
	if !strings.Contains(repairTaskIDsJSON, taskID) {
		t.Fatalf("repair follow-up task ids did not surface source task: %s", repairTaskIDsJSON)
	}

	var queueTaskID, queueAgentID, queuePayload string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT task_id, agent_id, payload_json
		  FROM operator_queue_items
		 WHERE workspace_id = ? AND source_kind = 'tension' AND source_id = ?
		 ORDER BY created_at DESC
		 LIMIT 1`,
		workspaceID,
		repairID,
	).Scan(&queueTaskID, &queueAgentID, &queuePayload); err != nil {
		t.Fatalf("query rebase follow-up queue task context: %v", err)
	}
	if queueTaskID != taskID {
		t.Fatalf("queue task id = %s, want %s", queueTaskID, taskID)
	}
	if queueAgentID != "agent-a" {
		t.Fatalf("queue agent id = %s, want %s", queueAgentID, "agent-a")
	}
	if !strings.Contains(queuePayload, `"task_id":"`+taskID+`"`) {
		t.Fatalf("queue payload did not surface task id: %s", queuePayload)
	}
	if !strings.Contains(queuePayload, `"task_ids":["`+taskID+`"]`) {
		t.Fatalf("queue payload did not surface task ids: %s", queuePayload)
	}
	steward, err := store.GetActiveSteward(ctx, "task:"+workspaceID+"/"+taskID)
	if err != nil {
		t.Fatalf("GetActiveSteward: %v", err)
	}
	if steward.StewardAgentID != "agent-a" {
		t.Fatalf("bundle steward agent = %s, want %s", steward.StewardAgentID, "agent-a")
	}
	if steward.EpochID != repairID {
		t.Fatalf("bundle steward epoch = %s, want %s", steward.EpochID, repairID)
	}
}

func requireRuntimeEventAuthorityMetadata(t *testing.T, store *Store, ctx context.Context, workspaceID, eventType, entityID string, authority WorkspaceAuthorityRecord) {
	t.Helper()

	var holderNodeID, leaseFingerprint string
	var authorityTerm int64
	if err := store.DB().QueryRowContext(ctx, `
		SELECT authority_holder_node_id, authority_term, authority_lease_token_fingerprint
		  FROM runtime_events
		 WHERE workspace_id = ? AND event_type = ? AND entity_id = ?
		 ORDER BY COALESCE(ingest_seq,0) DESC, created_at DESC, event_id DESC
		 LIMIT 1`,
		workspaceID,
		eventType,
		entityID,
	).Scan(&holderNodeID, &authorityTerm, &leaseFingerprint); err != nil {
		t.Fatalf("query %s authority metadata for %s: %v", eventType, entityID, err)
	}

	if holderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("%s authority holder = %s, want %s", eventType, holderNodeID, authority.HolderAuthorityNodeID)
	}
	if authorityTerm != authority.Term {
		t.Fatalf("%s authority term = %d, want %d", eventType, authorityTerm, authority.Term)
	}
	expectedFingerprint := authorityLeaseTokenFingerprint(authority.LeaseToken)
	if leaseFingerprint != expectedFingerprint {
		t.Fatalf("%s authority lease fingerprint = %s, want %s", eventType, leaseFingerprint, expectedFingerprint)
	}
}

func TestEvaluateCoalitionBundleTaskLinkedRebaseFollowupRequiresStewardCandidate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-bundle-missing-steward-candidate"
	taskID := "task-bundle-missing-steward-candidate"
	tensionID := "tension:task:" + workspaceID + "/" + taskID

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-bundle-missing-steward-candidate")
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      tensionID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: "task:" + workspaceID + "/" + taskID,
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Task-linked bundle source without agent",
		AnchorKind:     "task",
		AnchorRef:      taskID,
		TaskIDs:        []string{taskID},
		BaseScore:      70,
		SurfaceScore:   80,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Task-linked bundle missing steward candidate")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}

	_, err = store.EvaluateCoalitionBundle(ctx, workspaceID, coalition.CoalitionID, BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		RedundancyScore:                0.9,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaRedundancy:                0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.85,
		AdmissionSoftConflictThreshold: 0.9,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-missing-steward-candidate")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "steward candidate") {
		t.Fatalf("expected missing steward candidate failure, got %v", err)
	}

	var generatedCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(1)
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type IN ('fork_candidate', 'repair')
	`, workspaceID).Scan(&generatedCount); err != nil {
		t.Fatalf("count generated tensions after failed bundle evaluation: %v", err)
	}
	if generatedCount != 0 {
		t.Fatalf("expected failed task-linked bundle without steward candidate to roll back generated tensions, got %d", generatedCount)
	}
}

func TestEvaluateCoalitionBundleSkipsRepairFollowupWhenHardForkRequired(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-bundle-hard-fork-followup"

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	decision, err := store.EvaluateCoalitionBundle(ctx, workspaceID, "coalition-r10-hard", BundleUtilityParams{
		BaseValue:                      0.4,
		UnlockScore:                    0.6,
		CoverageScore:                  0.6,
		SoftConflictScore:              0.8,
		VerifierFailsCount:             1,
		AlphaUnlock:                    0.2,
		AlphaCoverage:                  0.2,
		AlphaSoftConflict:              0.05,
		MergeThreshold:                 0.2,
		MergeBaseThreshold:             0.2,
		MergeUnlockThreshold:           0.4,
		MergeCoverageThreshold:         0.4,
		AdmissionRedundancyThreshold:   0.95,
		AdmissionSoftConflictThreshold: 0.75,
		AdmissionLeaseRiskThreshold:    0.9,
		AdmissionCombinedRiskThreshold: 0.95,
	}, "patch-ref-hard")
	if err != nil {
		t.Fatalf("EvaluateCoalitionBundle: %v", err)
	}
	if decision.NextAction != "hard_fork" {
		t.Fatalf("next action = %s, want hard_fork", decision.NextAction)
	}
	if decision.RebaseAdmissible {
		t.Fatal("expected rebase admission to be blocked")
	}

	var repairCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM workspace_tensions
		 WHERE workspace_id = ? AND tension_type = 'repair'`,
		workspaceID,
	).Scan(&repairCount); err != nil {
		t.Fatalf("count repair follow-up tensions: %v", err)
	}
	if repairCount != 0 {
		t.Fatalf("repair follow-up count = %d, want 0", repairCount)
	}

	var queueCount int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM operator_queue_items
		 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&queueCount); err != nil {
		t.Fatalf("count operator queue items: %v", err)
	}
	if queueCount != 0 {
		t.Fatalf("operator queue count = %d, want 0", queueCount)
	}
}

func TestCoalitionConcurrencyStorm(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-storm-test"

	// Create 15 agents
	agentIDs := make([]string, 15)
	for i := 0; i < 15; i++ {
		agentIDs[i] = fmt.Sprintf("agent-x-%d", i)
	}

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentIDs...)
	ensureTensionOverlayTables(t, ctx, store)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	// 1. Force the cluster into STABILIZE mode
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_cluster_control_state
		(workspace_id, proto_cluster_id, current_mode, candidate_mode, candidate_streak, updated_at, created_at)
		VALUES (?, 'cluster-01', 'STABILIZE', 'STABILIZE', 1, ?, ?)`, workspaceID, now, now)
	if err != nil {
		t.Fatalf("Failed to set cluster to STABILIZE: %v", err)
	}

	// 2. Create a canonical meta tension
	tensionID := "tension:meta:storm1"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "meta-tension",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	// Pre-create coalition so all agents slam the exact same one
	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Survive the storm")
	if err != nil {
		t.Fatalf("Failed to create coalition: %v", err)
	}

	memIDShared := "mem-shared-1"
	if _, err := store.WriteMemoryNode(ctx, MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryID:    memIDShared + "-a",
		MemoryType:  "ENTITY",
		Body:        "shared overlap A",
		AgentID:     agentIDs[0],
		SourceKind:  "storm-shared",
		SourceID:    "shared-origin",
	}); err != nil {
		t.Fatalf("write shared memory node for %s: %v", agentIDs[0], err)
	}
	if _, err := store.WriteMemoryNode(ctx, MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryID:    memIDShared + "-b",
		MemoryType:  "ENTITY",
		Body:        "shared overlap B",
		AgentID:     agentIDs[1],
		SourceKind:  "storm-shared",
		SourceID:    "shared-origin",
	}); err != nil {
		t.Fatalf("write shared memory node for %s: %v", agentIDs[1], err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 15*3) // generous buffer
	start := make(chan struct{})

	// 3. Launch 15 agents concurrently
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(agentIndex int) {
			defer wg.Done()
			agent := agentIDs[agentIndex]
			<-start

			// A. List Tensions (Concurrency Read)
			_, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, agent)
			if err != nil {
				errs <- fmt.Errorf("agent %s list error: %v", agent, err)
				return
			}

			// B. Add Member (Concurrency Write to Coalition)
			err = runCoalitionStormWithRetry(ctx, func() error {
				return store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agent, 0.8, 0.5)
			})
			if err != nil {
				if !errors.Is(err, ErrCoalitionCapacityReached) {
					errs <- fmt.Errorf("agent %s attach error: %v", agent, err)
					return
				}
			}

			// C. Evaluate Bundle logic (Concurrency Check Penalty/FORK)
			// Trigger alternating Merge/Fork checks
			fails := 0
			if agentIndex%3 == 0 {
				fails = 5 // Guaranteed to drop U(B) below threshold and cause FORK
			}
			params := BundleUtilityParams{
				BaseValue:            1.5,
				ContradictEdgesCount: 1,
				VerifierFailsCount:   fails,
				Lambda1:              0.5,
				Lambda2:              2.0,
				MergeThreshold:       0.1,
			}
			_, err = runCoalitionStormEvaluateWithRetry(ctx, store, workspaceID, coalition.CoalitionID, params, "patch_ref_"+agent)
			if err != nil {
				errs <- fmt.Errorf("agent %s evaluate error: %v", agent, err)
			}
		}(i)
	}
	close(start)

	wg.Wait()
	close(errs)

	hasErr := false
	for err := range errs {
		t.Errorf("Concurrent storm error: %v", err)
		hasErr = true
	}
	if hasErr {
		t.Fatalf("Storm failed due to concurrency errors (likely database is locked)")
	}

	// 4. Verify outcomes
	finalCoalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("Failed to fetch final coalition: %v", err)
	}

	if len(finalCoalition.Members) != 4 {
		t.Errorf("Expected coalition size cap of 4 members, got %d", len(finalCoalition.Members))
	}

	// Check if fork_candidate tensions were generated directly from DB
	var forkCount int
	err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM workspace_tensions WHERE tension_type = 'fork_candidate'").Scan(&forkCount)
	if err != nil {
		t.Fatalf("Failed to count forks directly: %v", err)
	}
	if forkCount == 0 {
		t.Errorf("Expected multiple fork_candidate tensions to be generated during the storm")
	}

	// Check roles distribution
	rolesCount := map[string]int{}
	for _, m := range finalCoalition.Members {
		rolesCount[m.Role]++
	}

	t.Logf("Roles Distribution after storm: %v", rolesCount)
	t.Logf("Forks Generated: %d", forkCount)

	if rolesCount["GENERATOR"] != 1 {
		t.Errorf("Expected exactly ONE generator, but got %d (Concurrency race condition?)", rolesCount["GENERATOR"])
	}
}

func runCoalitionStormEvaluateWithRetry(ctx context.Context, store *Store, workspaceID, coalitionID string, params BundleUtilityParams, alternativePatchRef string) (BundleDecision, error) {
	var decision BundleDecision
	err := runCoalitionStormWithRetry(ctx, func() error {
		var err error
		decision, err = store.EvaluateCoalitionBundle(ctx, workspaceID, coalitionID, params, alternativePatchRef)
		return err
	})
	return decision, err
}

func runCoalitionStormWithRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil || !isCoalitionStormRetryable(lastErr) {
			return lastErr
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	return lastErr
}

func isCoalitionStormRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "database schema is locked") ||
		strings.Contains(msg, "database is busy")
}
