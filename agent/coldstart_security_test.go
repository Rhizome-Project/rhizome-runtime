package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestIsTransientYieldableExecError locks CW-A1/A2/A3: transient/recoverable execution-cycle errors
// (provider timeout, transient budget reserve/capture RPC blip, transport/5xx/inner-deadline) must be
// yieldable (released for retry), while genuine budget exhaustion and the cycle's OWN deadline must NOT
// be yielded (they terminalize / block).
func TestIsTransientYieldableExecError(t *testing.T) {
	bg := context.Background()
	transientBudget := fmt.Errorf("%w: reserve llm provider budget: rpc reserve_budget http 503 service unavailable", errRuntimeBudgetExhausted)
	captureBudget := fmt.Errorf("%w: state=spend: capture llm provider spend after provider success: rpc x: transport: connection reset", errRuntimeBudgetExhausted)
	genuineDaily := fmt.Errorf("%w: daily token budget exhausted for limit group g1", errRuntimeBudgetExhausted)
	genuineUsage := fmt.Errorf("%w: llm provider usage exceeded reserved budget: estimated_spend_micros=10 reserved_micros=5", errRuntimeBudgetExhausted)
	providerTimeout := fmt.Errorf("iteration 1: llm provider call timed out after 10m0s: %w", context.DeadlineExceeded)
	providerCapacity := fmt.Errorf("iteration 1: codex exec failed: exit status 1 (output: ERROR: Selected model is at capacity. Please try a different model.)")
	transportErr := fmt.Errorf("rpc agent.work.next: transport: connection reset by peer")

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"provider_timeout", providerTimeout, true},
		{"provider_capacity", providerCapacity, true},
		{"transient_budget_reserve_503", transientBudget, true},
		{"transient_budget_capture_transport", captureBudget, true},
		{"generic_transport", transportErr, true},
		{"genuine_daily_exhaustion_blocks", genuineDaily, false},
		{"genuine_usage_exceeded_blocks", genuineUsage, false},
	}
	for _, c := range cases {
		if got := isTransientYieldableExecError(bg, c.err); got != c.want {
			t.Errorf("%s: isTransientYieldableExecError=%v want %v", c.name, got, c.want)
		}
	}

	// The task cycle's OWN deadline must terminalize, never infinite-yield, except when the
	// error itself proves a provider-transient failure that consumed the same deadline.
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if !isTransientYieldableExecError(expired, providerTimeout) {
		t.Error("expired cycle ctx with explicit provider timeout marker must still yield/release")
	}
	if isTransientYieldableExecError(expired, transientBudget) {
		t.Error("cycle-deadline ctx must not be treated as yieldable (must terminalize)")
	}
}

// TestAuthorityTransitionRecognizesRepositoryRepairCarrier locks CW-B1: a dedicated repository-repair
// authority carrier (admitted by storage projectRepositoryRepairTask / CR-49) is also recognized by the
// agent authority-transition predicate, so it is not hard-declined on the sender/receiver face.
func TestAuthorityTransitionRecognizesRepositoryRepairCarrier(t *testing.T) {
	repoRepair := WorkspaceTaskRecord{
		TaskID:      "task-role-scope-repo-1",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-1",
		ProjectLane: "coordination",
		Tags:        []string{"project-repo-repair"},
	}
	if !authorityTransitionTaskLooksDedicated(repoRepair) {
		t.Error("repository-repair authority carrier should be recognized as dedicated")
	}

	repoRepairNoProject := repoRepair
	repoRepairNoProject.ProjectID = ""
	if authorityTransitionTaskLooksDedicated(repoRepairNoProject) {
		t.Error("repo-repair carrier without project_id must not be recognized as dedicated")
	}

	plainImpl := WorkspaceTaskRecord{
		TaskID:      "task-impl-1",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-1",
		ProjectLane: "implementation",
		Tags:        []string{"implementation"},
	}
	if authorityTransitionTaskLooksDedicated(plainImpl) {
		t.Error("plain implementation task must not be recognized as a dedicated authority carrier")
	}
}
