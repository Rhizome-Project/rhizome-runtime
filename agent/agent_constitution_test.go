package main

import (
	"strings"
	"testing"
)

// --- helpers ---------------------------------------------------------------

// constitutionTestEvent builds an explicitly-typed procedure/anti-procedure
// task_cycle_result event for a given locus. Because the procedure signature
// encodes the anchor, varying taskID/sessionID forces the action-level
// aggregate to observe multiple distinct loci (the generalization signal).
func constitutionTestEvent(node localMemoryNodeType, action, taskID, sessionID, occurredAt string) LocalMemoryEvent {
	outcome := "completed"
	memType := "PROCEDURE"
	if node == localMemoryNodeAntiProcedure {
		outcome = "blocked"
		memType = "ANTI_PROCEDURE"
	}
	return LocalMemoryEvent{
		NodeType:   node,
		EventKind:  "task_cycle_result",
		Summary:    action,
		Details:    action,
		TaskID:     taskID,
		SessionID:  sessionID,
		Outcome:    outcome,
		OccurredAt: occurredAt,
		MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
			Outcome:    outcome,
			MemoryType: memType,
			NextAction: action,
		}),
	}
}

func findProcedureByAction(hints map[string]LocalProcedureMemory, action string) (LocalProcedureMemory, bool) {
	for _, hint := range hints {
		if strings.Contains(strings.ToLower(hint.Summary), strings.ToLower(action)) {
			return hint, true
		}
	}
	return LocalProcedureMemory{}, false
}

// --- deriveLocalProcedureMaps Global flagging ------------------------------

func TestDeriveProceduresGlobalAcrossDistinctLoci(t *testing.T) {
	// Same explicit action observed in 3 distinct task loci, evidence >= min(3).
	events := []LocalMemoryEvent{
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-a", "sess-a", "2026-05-01T00:00:00Z"),
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-b", "sess-b", "2026-05-02T00:00:00Z"),
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-c", "sess-c", "2026-05-03T00:00:00Z"),
	}
	procedures, _ := deriveLocalProcedureMaps(events, constitutionDefaultMinEvidence)
	hint, ok := findProcedureByAction(procedures, "run gofmt before commit")
	if !ok {
		t.Fatalf("expected derived procedure for repeated action, got %+v", procedures)
	}
	if hint.DistinctAnchors < 2 {
		t.Fatalf("DistinctAnchors = %d, want >= 2", hint.DistinctAnchors)
	}
	if !hint.Global {
		t.Fatalf("expected Global=true for explicit action across 3 loci with evidence>=min, got %+v", hint)
	}
}

func TestDeriveProceduresSingleLocusNotGlobal(t *testing.T) {
	// Same explicit action but only ONE task locus, repeated 3 times.
	events := []LocalMemoryEvent{
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-a", "sess-a", "2026-05-01T00:00:00Z"),
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-a", "sess-a", "2026-05-02T00:00:00Z"),
		constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", "task-a", "sess-a", "2026-05-03T00:00:00Z"),
	}
	procedures, _ := deriveLocalProcedureMaps(events, constitutionDefaultMinEvidence)
	hint, ok := findProcedureByAction(procedures, "run gofmt before commit")
	if !ok {
		t.Fatalf("expected derived procedure, got %+v", procedures)
	}
	if hint.Global {
		t.Fatalf("single-locus action must NOT be Global, got %+v", hint)
	}
}

func TestDeriveProceduresImplicitAcrossLociNotGlobal(t *testing.T) {
	// Implicit evidence (no explicit node type / memory_type), across 3 loci.
	mk := func(taskID, sessionID, occurredAt string) LocalMemoryEvent {
		return LocalMemoryEvent{
			NodeType:   localMemoryNodeArtifactDelta, // not procedure/anti
			EventKind:  "task_cycle_result",
			Summary:    "refresh scratch buffer",
			Details:    "refresh scratch buffer",
			TaskID:     taskID,
			SessionID:  sessionID,
			Outcome:    "completed",
			OccurredAt: occurredAt,
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "refresh scratch buffer",
			}),
		}
	}
	// Implicit evidence needs >=2 occurrences in the SAME (per-anchor) bucket to
	// derive at all, so each locus appears twice. Two distinct loci => the action
	// generalizes, but implicit evidence must still never become Global.
	events := []LocalMemoryEvent{
		mk("task-a", "sess-a", "2026-05-01T00:00:00Z"),
		mk("task-a", "sess-a", "2026-05-02T00:00:00Z"),
		mk("task-b", "sess-b", "2026-05-03T00:00:00Z"),
		mk("task-b", "sess-b", "2026-05-04T00:00:00Z"),
	}
	procedures, _ := deriveLocalProcedureMaps(events, constitutionDefaultMinEvidence)
	hint, ok := findProcedureByAction(procedures, "refresh scratch buffer")
	if !ok {
		t.Fatalf("expected derived implicit procedure, got %+v", procedures)
	}
	if hint.Global {
		t.Fatalf("implicit evidence must never become Global, got %+v", hint)
	}
}

func TestDeriveProceduresExplicitAcrossLociBelowEvidenceNotGlobal(t *testing.T) {
	// Explicit action across 2 loci => distinctAnchors=2, but evidence (2) < min(3).
	events := []LocalMemoryEvent{
		constitutionTestEvent(localMemoryNodeAntiProcedure, "force push to main", "task-a", "sess-a", "2026-05-01T00:00:00Z"),
		constitutionTestEvent(localMemoryNodeAntiProcedure, "force push to main", "task-b", "sess-b", "2026-05-02T00:00:00Z"),
	}
	_, antiProcedures := deriveLocalProcedureMaps(events, constitutionDefaultMinEvidence)
	hint, ok := findProcedureByAction(antiProcedures, "force push to main")
	if !ok {
		t.Fatalf("expected derived anti-procedure, got %+v", antiProcedures)
	}
	if hint.DistinctAnchors < 2 {
		t.Fatalf("DistinctAnchors = %d, want 2", hint.DistinctAnchors)
	}
	if hint.Global {
		t.Fatalf("evidence below min must NOT be Global, got %+v", hint)
	}
	// Lowering the threshold to 2 should now promote it.
	_, antiProcedures2 := deriveLocalProcedureMaps(events, 2)
	hint2, _ := findProcedureByAction(antiProcedures2, "force push to main")
	if !hint2.Global {
		t.Fatalf("with min_evidence=2 the cross-loci anti-procedure should be Global, got %+v", hint2)
	}
}

// --- selectConstitutionRules -----------------------------------------------

func globalHint(node localMemoryNodeType, hintID, summary string, evidence int) LocalProcedureMemory {
	return LocalProcedureMemory{
		HintID:          hintID,
		NodeType:        node,
		Summary:         summary,
		Guidance:        summary,
		EvidenceCount:   evidence,
		DistinctAnchors: 3,
		Global:          true,
	}
}

func TestSelectConstitutionRulesSeedsRankFirst(t *testing.T) {
	procedures := map[string]LocalProcedureMemory{
		"p1": globalHint(localMemoryNodeProcedure, "p1", "prefer derived path", 9),
	}
	antiProcedures := map[string]LocalProcedureMemory{
		"a1": globalHint(localMemoryNodeAntiProcedure, "a1", "avoid derived path", 9),
	}
	seeds := []AgentAnatomyConstitutionRule{
		{ID: "s1", Rule: "never delete production data", Kind: "invariant", Priority: 1},
	}
	out := selectConstitutionRules(procedures, antiProcedures, seeds, 7)
	if len(out) != 3 {
		t.Fatalf("expected 3 rules, got %d: %+v", len(out), out)
	}
	if !out[0].Seed || out[0].Text != "never delete production data" {
		t.Fatalf("seed must rank first, got %+v", out[0])
	}
}

func TestSelectConstitutionRulesCapPrefersSafetyOverProcedure(t *testing.T) {
	procedures := map[string]LocalProcedureMemory{
		"p1": globalHint(localMemoryNodeProcedure, "p1", "prefer derived path", 9),
	}
	antiProcedures := map[string]LocalProcedureMemory{
		"a1": globalHint(localMemoryNodeAntiProcedure, "a1", "avoid derived path", 5),
	}
	// Cap of 1: only the derived anti (safety) should survive among derived rules.
	out := selectConstitutionRules(procedures, antiProcedures, nil, 1)
	if len(out) != 1 {
		t.Fatalf("cap=1 should yield 1 rule, got %d: %+v", len(out), out)
	}
	if out[0].Kind != "anti_procedure" {
		t.Fatalf("on truncation anti_procedure must win over procedure, got %+v", out[0])
	}
}

func TestSelectConstitutionRulesDedupesByKindAndText(t *testing.T) {
	// Two distinct buckets globalizing the same action/text (different hint IDs).
	procedures := map[string]LocalProcedureMemory{
		"p1": globalHint(localMemoryNodeProcedure, "p1", "run gofmt before commit", 4),
		"p2": globalHint(localMemoryNodeProcedure, "p2", "Run Gofmt Before Commit", 9),
	}
	out := selectConstitutionRules(procedures, nil, nil, 7)
	if len(out) != 1 {
		t.Fatalf("expected dedupe to a single rule, got %d: %+v", len(out), out)
	}
	if out[0].EvidenceCount != 9 {
		t.Fatalf("highest-ranked (evidence 9) instance should win, got %+v", out[0])
	}
}

func TestSelectConstitutionRulesExcludesNonGlobalDerived(t *testing.T) {
	notGlobal := globalHint(localMemoryNodeProcedure, "p1", "do not surface me", 9)
	notGlobal.Global = false
	procedures := map[string]LocalProcedureMemory{"p1": notGlobal}
	out := selectConstitutionRules(procedures, nil, nil, 7)
	if len(out) != 0 {
		t.Fatalf("non-global derived rules must be excluded, got %+v", out)
	}
}

func TestSelectConstitutionRulesZeroCapEmpty(t *testing.T) {
	seeds := []AgentAnatomyConstitutionRule{{ID: "s1", Rule: "x", Kind: "invariant"}}
	if out := selectConstitutionRules(nil, nil, seeds, 0); out != nil {
		t.Fatalf("maxRules=0 must return nil, got %+v", out)
	}
}

// --- agent_anatomy.go constitution config ----------------------------------

func TestNormalizeConstitutionDefaultsAndClamps(t *testing.T) {
	got := normalizeAgentAnatomyConstitution(
		AgentAnatomyConstitution{MaxRules: 999, MinEvidence: 999, Promotion: "bogus"},
		AgentAnatomyConstitution{},
	)
	if got.MaxRules != constitutionMaxRulesCeiling {
		t.Fatalf("MaxRules clamp = %d, want %d", got.MaxRules, constitutionMaxRulesCeiling)
	}
	if got.MinEvidence != constitutionMinEvidenceCeiling {
		t.Fatalf("MinEvidence clamp = %d, want %d", got.MinEvidence, constitutionMinEvidenceCeiling)
	}
	if got.Promotion != constitutionDefaultPromotion {
		t.Fatalf("Promotion fallback = %q, want %q", got.Promotion, constitutionDefaultPromotion)
	}

	empty := normalizeAgentAnatomyConstitution(AgentAnatomyConstitution{}, AgentAnatomyConstitution{})
	if empty.MaxRules != constitutionDefaultMaxRules {
		t.Fatalf("default MaxRules = %d, want %d", empty.MaxRules, constitutionDefaultMaxRules)
	}
	if empty.MinEvidence != constitutionDefaultMinEvidence {
		t.Fatalf("default MinEvidence = %d, want %d", empty.MinEvidence, constitutionDefaultMinEvidence)
	}
}

func TestNormalizeConstitutionSeedRulesTrimDropDedupe(t *testing.T) {
	seeds := normalizeConstitutionSeedRules([]AgentAnatomyConstitutionRule{
		{ID: " s1 ", Rule: "  keep me  ", Kind: ""},
		{ID: "s1", Rule: "dupe by id"}, // dropped: dup ID
		{ID: "", Rule: "no kind defaults to invariant"},
		{ID: "", Rule: " "},                             // dropped: empty rule
		{ID: "", Rule: "no kind defaults to invariant"}, // dropped: dup text
	})
	if len(seeds) != 2 {
		t.Fatalf("expected 2 seed rules after dedupe, got %d: %+v", len(seeds), seeds)
	}
	if seeds[0].ID != "s1" || seeds[0].Rule != "keep me" || seeds[0].Kind != "invariant" {
		t.Fatalf("first seed not normalized: %+v", seeds[0])
	}
}

func TestValidateConstitutionRejectsBadValues(t *testing.T) {
	if err := validateAgentAnatomyConstitution(AgentAnatomyConstitution{MaxRules: constitutionMaxRulesCeiling + 1}); err == nil {
		t.Fatalf("expected error for MaxRules over ceiling")
	}
	if err := validateAgentAnatomyConstitution(AgentAnatomyConstitution{MinEvidence: -1}); err == nil {
		t.Fatalf("expected error for negative MinEvidence")
	}
	if err := validateAgentAnatomyConstitution(AgentAnatomyConstitution{Promotion: "nonsense"}); err == nil {
		t.Fatalf("expected error for bad promotion mode")
	}
	if err := validateAgentAnatomyConstitution(AgentAnatomyConstitution{
		SeedRules: []AgentAnatomyConstitutionRule{{Rule: "x", Kind: "weird"}},
	}); err == nil {
		t.Fatalf("expected error for bad seed kind")
	}
	if err := validateAgentAnatomyConstitution(AgentAnatomyConstitution{
		MaxRules: 5, MinEvidence: 3, Promotion: "evidence_or_seed",
		SeedRules: []AgentAnatomyConstitutionRule{{Rule: "ok", Kind: "invariant"}},
	}); err != nil {
		t.Fatalf("valid constitution rejected: %v", err)
	}
}

func TestAgentAnatomyDigestSeedOrderStable(t *testing.T) {
	base := DefaultAgentAnatomyConfig(DefaultAgentProfile("iota", "Iota", "generalist implementer"))
	a := base
	a.Memory.Constitution.SeedRules = []AgentAnatomyConstitutionRule{
		{ID: "b", Rule: "beta", Kind: "invariant"},
		{ID: "a", Rule: "alpha", Kind: "invariant"},
	}
	b := base
	b.Memory.Constitution.SeedRules = []AgentAnatomyConstitutionRule{
		{ID: "a", Rule: "alpha", Kind: "invariant"},
		{ID: "b", Rule: "beta", Kind: "invariant"},
	}
	if AgentAnatomyDigest(a) != AgentAnatomyDigest(b) {
		t.Fatalf("digest must be stable regardless of seed order")
	}
}

// --- memory_packet.go injection --------------------------------------------

func setupConstitutionPacketService(t *testing.T, ws, agent string) *AgentMemoryService {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	return openTestAgentMemoryService(t, ws, agent)
}

func TestBuildPacketOmitsConstitutionWhenDisabled(t *testing.T) {
	service := setupConstitutionPacketService(t, "ws-const-off", "agent-const-off")
	seeds := []AgentAnatomyConstitutionRule{{ID: "s1", Rule: "never delete production data", Kind: "invariant"}}
	// Disabled (default): no Enabled pointer -> section absent.
	service.SetConstitution(AgentAnatomyConstitution{
		Enabled:   boolPtr(false),
		MaxRules:  7,
		Promotion: "evidence_or_seed",
		SeedRules: seeds,
	})
	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "t1", Title: "T", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "s1", TaskID: "t1", Status: "ACTIVE"},
	}, 4000)
	if strings.Contains(packet, "Constitution (Always-On Rules)") {
		t.Fatalf("constitution section must be absent when disabled, got:\n%s", packet)
	}
}

func TestBuildPacketInjectsSeedConstitutionWhenEnabled(t *testing.T) {
	service := setupConstitutionPacketService(t, "ws-const-on", "agent-const-on")
	service.SetConstitution(AgentAnatomyConstitution{
		Enabled:   boolPtr(true),
		MaxRules:  7,
		Promotion: "evidence_or_seed",
		SeedRules: []AgentAnatomyConstitutionRule{
			{ID: "s1", Rule: "never delete production data", Kind: "invariant"},
		},
	})
	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "t1", Title: "T", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "s1", TaskID: "t1", Status: "ACTIVE"},
	}, 4000)
	if !strings.Contains(packet, "Constitution (Always-On Rules)") {
		t.Fatalf("expected constitution section, got:\n%s", packet)
	}
	if !strings.Contains(packet, "[invariant] never delete production data") {
		t.Fatalf("expected invariant seed rendered, got:\n%s", packet)
	}
}

func TestBuildPacketSeedOnlyModeSuppressesDerived(t *testing.T) {
	service := setupConstitutionPacketService(t, "ws-const-seedonly", "agent-const-seedonly")
	// Seed an explicit, cross-loci, high-evidence action that WOULD globalize.
	for i, locus := range []struct{ task, sess string }{
		{"task-a", "sess-a"}, {"task-b", "sess-b"}, {"task-c", "sess-c"},
	} {
		ev := constitutionTestEvent(localMemoryNodeProcedure, "run gofmt before commit", locus.task, locus.sess, "2026-05-0"+string(rune('1'+i))+"T00:00:00Z")
		if err := service.appendEvent(ev); err != nil {
			t.Fatalf("appendEvent error: %v", err)
		}
	}
	service.SetConstitution(AgentAnatomyConstitution{
		Enabled:   boolPtr(true),
		MaxRules:  7,
		Promotion: "seed_only",
		SeedRules: []AgentAnatomyConstitutionRule{{ID: "s1", Rule: "seed only rule", Kind: "invariant"}},
	})
	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "t1", Title: "T", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "s1", TaskID: "t1", Status: "ACTIVE"},
	}, 4000)
	if !strings.Contains(packet, "seed only rule") {
		t.Fatalf("seed rule should still render in seed_only mode, got:\n%s", packet)
	}
	if strings.Contains(packet, "[prefer] run gofmt before commit") {
		t.Fatalf("seed_only mode must suppress derived rules, got:\n%s", packet)
	}
}
