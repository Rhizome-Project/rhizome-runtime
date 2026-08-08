package main

import "testing"

func TestImmediateProjectContinuationResumeSignatureAllowsGateDemotedPublicationStep(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{CoordinationMode: CoordinationModeTrustFirst}}
	task := projectCompletionGateTask()
	session := AgentSessionStateRecord{SessionID: "session-build", AgentID: "agent-alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	result := demoteProjectImplementationCompletion(StructuredTaskResult{Outcome: "completed", Summary: "Done"}, "project branch branch-1 is ACTIVE, not READY_FOR_REVIEW")

	signature, ok := runtime.immediateProjectContinuationResumeSignature(task, session, "run-build", result, &TaskRunTrace{})
	if !ok || signature == "" {
		t.Fatalf("expected gate-demoted self-actionable publication step to queue immediate resume, ok=%v signature=%q", ok, signature)
	}
}

func TestProjectContinuationNextMoveAllowsNegatedExternalBlockerPhrase(t *testing.T) {
	nextMove := "This is self-actionable, not an external blocker: call project_branch_commit with push=true, then project_branch_review_ready."
	if !projectContinuationNextMoveLooksSelfActionable(nextMove) {
		t.Fatalf("negated external-blocker phrase should remain self-actionable: %q", nextMove)
	}
}
