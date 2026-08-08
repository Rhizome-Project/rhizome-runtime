package main

import (
	"strings"
	"testing"
)

func TestNormalizeAutonomyResultSuppressesRoutineHumanEscalation(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome:       "blocked",
		Summary:       "Need review before proceeding",
		RequiresHuman: true,
		DecisionType:  "review",
		OwnerAction:   "Review the changes",
		HumanReason:   "Need review",
	})

	if got.RequiresHuman {
		t.Fatalf("expected routine review escalation to be suppressed, got %+v", got)
	}
	if got.OwnerAction != "" || got.HumanReason != "" {
		t.Fatalf("expected owner-facing fields to be cleared after suppression, got %+v", got)
	}
	if !strings.Contains(strings.ToLower(got.Details), "suppressed by autonomy policy") {
		t.Fatalf("expected autonomy suppression note, got %+v", got)
	}
}

func TestNormalizeAutonomyResultKeepsCredentialGateHuman(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "OAuth login required to continue",
		BlockedOn: []BlockedRef{{Kind: "tool", Detail: "OAuth login required in browser"}},
	})

	if !got.RequiresHuman {
		t.Fatalf("expected credential gate to require human help, got %+v", got)
	}
	if got.DecisionType != "credential" {
		t.Fatalf("expected decision_type=credential, got %+v", got)
	}
	if got.OwnerAction == "" || got.HumanReason == "" {
		t.Fatalf("expected credential gate to fill owner action and human reason, got %+v", got)
	}
}

func TestNormalizeAutonomyResultSuppressesUnsubstantiatedCredentialEscalation(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome:       "continue",
		Summary:       "Draft materialized; no authorization step is needed.",
		RequiresHuman: true,
		DecisionType:  "credential",
	})

	if got.RequiresHuman {
		t.Fatalf("expected unsupported credential escalation to be suppressed, got %+v", got)
	}
	if got.DecisionType != "credential" {
		t.Fatalf("expected explicit decision type to remain available for evidence, got %+v", got)
	}
	if got.OwnerAction != "" || got.HumanReason != "" {
		t.Fatalf("expected owner-facing fields to stay clear after suppression, got %+v", got)
	}
}

func TestShouldSurfaceOperatorQueueOnlyForTrueHumanGate(t *testing.T) {
	if shouldSurfaceOperatorQueue(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "waiting on dependency",
		BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "another task must land first"}},
	}) {
		t.Fatal("expected routine dependency blocker not to surface an operator queue")
	}
	if shouldSurfaceOperatorQueue(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Project coordination mentions a stale payment gate, but the current task is blocked on repository context.",
		BlockedOn: []BlockedRef{{
			Kind:   "dependency",
			Detail: "canonical project repository profile is stale",
		}},
	}) {
		t.Fatal("expected free-text payment wording without a structured human gate not to surface an operator queue")
	}
	if shouldSurfaceOperatorQueue(StructuredTaskResult{
		Outcome:       "blocked",
		RequiresHuman: true,
		DecisionType:  "payment",
		Summary:       "payment required",
	}) {
		t.Fatal("expected unsubstantiated payment flag not to surface an operator queue")
	}
	if !shouldSurfaceOperatorQueue(StructuredTaskResult{
		Outcome:       "blocked",
		RequiresHuman: true,
		DecisionType:  "payment",
		Summary:       "billing portal checkout is blocked",
		BlockedOn:     []BlockedRef{{Kind: "payment", Detail: "external checkout page requires a card payment method"}},
	}) {
		t.Fatal("expected concrete external payment gate to surface an operator queue")
	}
	if !shouldSurfaceOperatorQueue(StructuredTaskResult{
		Outcome:       "blocked",
		RequiresHuman: true,
		DecisionType:  "approval",
		Summary:       "policy requires approval for privileged tool",
		HumanReason:   "policy requires approval for tool deploy",
	}) {
		t.Fatal("expected concrete policy approval gate to surface an operator queue")
	}
}

func TestShouldEndRoutineDependencyBlockedSessionOnlyForDependencyBlocks(t *testing.T) {
	if !shouldEndRoutineDependencyBlockedSession(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "waiting on implementation task",
		BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "gamma must publish review-ready branch"}},
	}) {
		t.Fatal("expected pure dependency blocker to end session without operator queue")
	}
	if shouldEndRoutineDependencyBlockedSession(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "runtime failed",
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: "session already ended"}},
	}) {
		t.Fatal("expected runtime blocker to keep existing blocked-session path")
	}
	if shouldEndRoutineDependencyBlockedSession(StructuredTaskResult{
		Outcome:       "blocked",
		RequiresHuman: true,
		DecisionType:  "credential",
		Summary:       "OAuth required",
		BlockedOn:     []BlockedRef{{Kind: "credential", Detail: "OAuth login required in browser"}},
	}) {
		t.Fatal("expected human gate to keep operator queue path")
	}
}

func TestNormalizeAutonomyResultTreatsPeerOwnershipConflictAsDependency(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "ownership conflict: implementation task is already claimed",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "ownership conflict: implementation task already claimed by delta and has ACTIVE checkout/branch evidence, so gamma has no permissible runnable lane this cycle",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected peer ownership conflict to normalize as dependency, got %+v", got)
	}
	if got.RequiresHuman || shouldSurfaceOperatorQueue(got) {
		t.Fatalf("expected peer ownership conflict to avoid operator queue, got %+v", got)
	}
	if !shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected peer ownership conflict to use routine dependency session ending, got %+v", got)
	}
}

func TestNormalizeAutonomyResultTreatsReviewEvidenceGapAsDependency(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "missing runtime/browser verification evidence",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "missing runtime/browser verification evidence for Canvas/ImageBitmap/ImageData APIs; reviewer or upstream verification lane must publish smoke evidence",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected review/smoke evidence gap to normalize as dependency, got %+v", got)
	}
	if got.RequiresHuman || shouldSurfaceOperatorQueue(got) {
		t.Fatalf("expected review/smoke evidence gap to avoid operator queue, got %+v", got)
	}
	if !shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected review/smoke evidence gap to end as routine dependency, got %+v", got)
	}
}

func TestNormalizeAutonomyResultTreatsReviewPacketGapAsDependency(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "review packet is incomplete",
		BlockedOn: []BlockedRef{{
			Kind:   "tool",
			Detail: "review packet lacks channel-mapping verification evidence from the pipeline branch",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected review packet evidence gap to normalize as dependency, got %+v", got)
	}
	if got.RequiresHuman || shouldSurfaceOperatorQueue(got) || !shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected review packet evidence gap to stay autonomous, got %+v", got)
	}
}

func TestNormalizeAutonomyResultPreservesExecutableFailureWithReviewEvidenceWords(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "browser smoke failed",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "browser verification evidence is missing because the local executable failure returned exit status 1 during smoke",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "runtime" {
		t.Fatalf("expected concrete executable failure to remain runtime, got %+v", got)
	}
	if shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected executable failure to keep blocked-session path, got %+v", got)
	}
}

func TestNormalizeAutonomyResultPreservesCredentialGateWithReviewEvidenceWords(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "browser verification needs auth",
		BlockedOn: []BlockedRef{{
			Kind:   "tool",
			Detail: "browser verification evidence requires OAuth login before smoke can run",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "credential" {
		t.Fatalf("expected concrete auth gate to remain credential, got %+v", got)
	}
	if !got.RequiresHuman || !shouldSurfaceOperatorQueue(got) {
		t.Fatalf("expected concrete auth gate to surface operator queue, got %+v", got)
	}
}

func TestNormalizeAutonomyResultAvoidsPaymentCollisionForUICardEvidence(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "missing UI smoke evidence",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "browser verification evidence for the UI card preview is missing until the reviewer publishes smoke evidence",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "dependency" {
		t.Fatalf("expected UI card review evidence gap to remain dependency, got %+v", got)
	}
	if got.RequiresHuman || shouldSurfaceOperatorQueue(got) {
		t.Fatalf("expected UI card wording not to surface payment queue, got %+v", got)
	}
}

func TestNormalizeAutonomyResultAvoidsApprovalCollisionForPermissionDenied(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "browser smoke executable failed",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "browser verification evidence is missing because the local executable failed with permission denied",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "runtime" {
		t.Fatalf("expected permission denied executable failure to remain runtime, got %+v", got)
	}
	if got.RequiresHuman || shouldSurfaceOperatorQueue(got) || shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected permission denied not to surface approval queue or dependency end, got %+v", got)
	}
}

func TestNormalizeAutonomyResultKeepsExplicitRuntimeBlockerDespiteBranchWords(t *testing.T) {
	r := &Runtime{}
	got := r.normalizeAutonomyResult(StructuredTaskResult{
		Outcome: "blocked",
		Summary: "project branch commit failed",
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: "project_branch_commit failed while inspecting the active checkout and branch evidence",
		}},
	})

	if len(got.BlockedOn) != 1 || got.BlockedOn[0].Kind != "runtime" {
		t.Fatalf("expected explicit runtime blocker to survive branch wording, got %+v", got)
	}
	if shouldEndRoutineDependencyBlockedSession(got) {
		t.Fatalf("expected explicit runtime blocker to keep blocked-session path, got %+v", got)
	}
}
