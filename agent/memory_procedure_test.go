package main

import (
	"testing"
)

func TestClassifyProcedureExplicitNodeType(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind: "some_random_kind",
		NodeType:  localMemoryNodeProcedure,
	}
	class, ok := classifyLocalProcedureEvent(event, "", "")
	if !ok {
		t.Fatal("expected explicit PROCEDURE to be classified")
	}
	if class.kind != localMemoryNodeProcedure {
		t.Fatalf("expected procedure, got %v", class.kind)
	}
	if !class.explicit {
		t.Fatal("expected explicit flag")
	}
}

func TestClassifyProcedureRuntimeRecovery(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind: "runtime_recovery",
		Summary:   "panic in worker loop",
	}
	class, ok := classifyLocalProcedureEvent(event, "", "")
	if !ok {
		t.Fatal("expected runtime_recovery to be classified")
	}
	if class.kind != localMemoryNodeAntiProcedure {
		t.Fatalf("expected anti-procedure, got %v", class.kind)
	}
}

func TestClassifyProcedureSessionDecision(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind: "session_decision_needed",
		Outcome:   "handoff",
		Details:   "could not find solution",
	}
	class, ok := classifyLocalProcedureEvent(event, "", "")
	if !ok {
		t.Fatal("expected session_decision_needed (handoff) to be classified")
	}
	if class.kind != localMemoryNodeAntiProcedure {
		t.Fatalf("expected anti-procedure, got %v", class.kind)
	}
}

func TestClassifyProcedureSessionDecisionIgnoresHumanGate(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind:     "session_decision_needed",
		Outcome:       "blocked",
		RequiresHuman: true,
		Details:       "waiting for approval",
	}
	_, ok := classifyLocalProcedureEvent(event, "", "")
	if ok {
		t.Fatal("expected human-blocked decision to NOT be classified")
	}
}

func TestClassifyProcedureIgnoresOtherEvents(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind: "system_news",
		Summary:   "some news",
	}
	_, ok := classifyLocalProcedureEvent(event, "", "")
	if ok {
		t.Fatal("expected generic event to be ignored")
	}
}

func TestBuildLocalProcedureSignatureGeneratesConsistently(t *testing.T) {
	event := LocalMemoryEvent{
		EventKind: "runtime_recovery",
		TaskID:    "task-1",
		Summary:   "panic",
	}
	class, ok := classifyLocalProcedureEvent(event, "", "")
	if !ok {
		t.Fatal("could not classify")
	}
	sig := buildLocalProcedureSignature(event, class)
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}
	expectedPrefix := string(localMemoryNodeAntiProcedure) + "|task=task-1|session=|tension=|cluster=|action=panic"
	if sig != expectedPrefix {
		t.Fatalf("expected signature %q, got %q", expectedPrefix, sig)
	}
}
