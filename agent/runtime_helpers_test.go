package main

import (
	"strings"
	"testing"
)

func TestChooseNextTaskPrefersOwnedThenPriority(t *testing.T) {
	other := "agent-other"
	self := "agent-self"
	claimed := "CLAIMED"

	tasks := []WorkspaceTaskRecord{
		{TaskID: "low-unclaimed", Priority: "LOW", Status: "PENDING"},
		{TaskID: "high-unclaimed", Priority: "HIGH", Status: "PENDING"},
		{TaskID: "owned", Priority: "LOW", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
		{TaskID: "other-owned", Priority: "CRITICAL", Status: "RUNNING", ClaimAgentID: &other, ClaimStatus: &claimed},
	}

	task := chooseNextTask(tasks, self)
	if task == nil {
		t.Fatal("chooseNextTask returned nil")
	}
	if task.TaskID != "owned" {
		t.Fatalf("expected owned task first, got %q", task.TaskID)
	}

	tasks = []WorkspaceTaskRecord{
		{TaskID: "low-unclaimed", Priority: "LOW", Status: "PENDING"},
		{TaskID: "critical-unclaimed", Priority: "CRITICAL", Status: "PENDING"},
	}
	task = chooseNextTask(tasks, self)
	if task == nil || task.TaskID != "critical-unclaimed" {
		t.Fatalf("expected critical unclaimed task, got %+v", task)
	}
}

func TestChooseNextTaskSkipsBlockedClaims(t *testing.T) {
	self := "agent-self"
	blocked := "BLOCKED"

	tasks := []WorkspaceTaskRecord{
		{TaskID: "blocked-owned", Priority: "CRITICAL", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &blocked},
	}

	if task := chooseNextTask(tasks, self); task != nil {
		t.Fatalf("expected blocked claim to be skipped, got %+v", task)
	}
}

func TestChooseNextTaskReclaimsReleasedSameOwner(t *testing.T) {
	self := "agent-self"
	other := "agent-other"
	released := "RELEASED"

	tasks := []WorkspaceTaskRecord{
		{TaskID: "critical-unclaimed", Priority: "CRITICAL", Status: "PENDING"},
		{TaskID: "released-foreign", Priority: "CRITICAL", Status: "PENDING", ClaimAgentID: &other, ClaimStatus: &released},
		{TaskID: "released-owned", Priority: "LOW", Status: "PENDING", ClaimAgentID: &self, ClaimStatus: &released},
	}

	task := chooseNextTask(tasks, self)
	if task == nil || task.TaskID != "released-owned" {
		t.Fatalf("expected released same-owner task to be reclaimable ahead of new work, got %+v", task)
	}
}

func TestChooseNextRunnableTaskSkipsWaitingDecisionSession(t *testing.T) {
	self := "agent-self"
	claimed := "CLAIMED"

	tasks := []WorkspaceTaskRecord{
		{TaskID: "waiting", Priority: "CRITICAL", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
		{TaskID: "ready", Priority: "HIGH", Status: "PENDING"},
	}
	sessions := []AgentSessionStateRecord{
		{SessionID: "session-1", AgentID: self, TaskID: "waiting", Status: "WAITING_DECISION"},
	}

	task := chooseNextRunnableTask(tasks, sessions, self)
	if task == nil || task.TaskID != "ready" {
		t.Fatalf("expected ready task, got %+v", task)
	}
}

func TestChooseNextRunnableTaskSkipsNonRunnableOwnedSession(t *testing.T) {
	self := "agent-self"
	claimed := "CLAIMED"

	tasks := []WorkspaceTaskRecord{
		{TaskID: "waiting", Priority: "CRITICAL", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
		{TaskID: "other", Priority: "HIGH", Status: "PENDING"},
	}
	sessions := []AgentSessionStateRecord{
		{SessionID: "sess-1", AgentID: self, TaskID: "waiting", Status: "WAITING_DECISION"},
	}

	got := chooseNextRunnableTask(tasks, sessions, self)
	if got == nil || got.TaskID != "other" {
		t.Fatalf("expected runnable fallback task, got %+v", got)
	}
}

func TestParseStructuredTaskResultFallsBackForPlainText(t *testing.T) {
	got := parseStructuredTaskResult("plain summary")
	if got.Outcome != "continue" {
		t.Fatalf("expected fallback outcome continue, got %q", got.Outcome)
	}
	if got.Summary != "plain summary" {
		t.Fatalf("unexpected summary: %q", got.Summary)
	}
}

func TestParseStructuredTaskResultNormalizesJSON(t *testing.T) {
	raw := `{"outcome":"COMPLETED","summary":" shipped ","memory_body":" note ","decision_type":" approval "}`
	got := parseStructuredTaskResult(raw)
	if got.Outcome != "completed" {
		t.Fatalf("expected completed outcome, got %q", got.Outcome)
	}
	if got.Summary != "shipped" {
		t.Fatalf("unexpected summary %q", got.Summary)
	}
	if got.MemoryBody != "note" {
		t.Fatalf("unexpected memory body %q", got.MemoryBody)
	}
	if got.DecisionType != "approval" {
		t.Fatalf("unexpected decision type %q", got.DecisionType)
	}
}

func TestParseStructuredTaskResultNormalizesReflection(t *testing.T) {
	raw := `{"outcome":"continue","summary":"working","reflection":{"current_intent":" ship the scaffold ","fresh_evidence":" npm test passed ","blocker_freshness":" no current blocker ","next_useful_move":" request review "}}`
	got := parseStructuredTaskResult(raw)
	if got.Reflection == nil {
		t.Fatalf("expected reflection to parse, got %+v", got)
	}
	if got.Reflection.CurrentIntent != "ship the scaffold" ||
		got.Reflection.FreshEvidence != "npm test passed" ||
		got.Reflection.BlockerFreshness != "no current blocker" ||
		got.Reflection.NextUsefulMove != "request review" {
		t.Fatalf("unexpected normalized reflection %+v", got.Reflection)
	}
}

func TestParseStructuredTaskResultStrictRejectsMalformedJSON(t *testing.T) {
	if _, err := parseStructuredTaskResultStrict(`{"outcome":"completed",`); err == nil {
		t.Fatal("expected malformed JSON to fail strict parsing")
	}
}

func TestParseStructuredTaskResultStrictRejectsUnexpectedFields(t *testing.T) {
	raw := `{"outcome":"completed","summary":"done","unexpected":"value"}`
	if _, err := parseStructuredTaskResultStrict(raw); err == nil {
		t.Fatal("expected strict parsing to reject unexpected top-level fields")
	}
}

func TestParseStructuredTaskResultStrictAcceptsNullOptionalFields(t *testing.T) {
	raw := `{"outcome":"completed","summary":"done","details":null,"materialize":null,"blocked_on":null}`
	got, err := parseStructuredTaskResultStrict(raw)
	if err != nil {
		t.Fatalf("expected null optional fields to remain compatible, got %v", err)
	}
	if got.Outcome != "completed" || got.Summary != "done" {
		t.Fatalf("unexpected parsed result %+v", got)
	}
}

func TestParseStructuredTaskResultStrictAcceptsReflection(t *testing.T) {
	raw := `{"outcome":"continue","summary":"still useful","reflection":{"current_intent":"build","fresh_evidence":"files changed","blocker_freshness":"none","next_useful_move":"test"}}`
	got, err := parseStructuredTaskResultStrict(raw)
	if err != nil {
		t.Fatalf("expected reflection to remain compatible, got %v", err)
	}
	if got.Reflection == nil || got.Reflection.NextUsefulMove != "test" {
		t.Fatalf("unexpected reflection result %+v", got)
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectSalvagesTrailingText(t *testing.T) {
	raw := "```json\n" +
		`{"outcome":"completed","summary":"done","details":"brace in string } is ok","reflection":{"current_intent":"check","fresh_evidence":"docs","blocker_freshness":"none","next_useful_move":"stop"}}` +
		"\n```\nextra commentary"
	got, salvaged, err := parseStructuredTaskResultStrictOrFirstObject(raw)
	if err != nil {
		t.Fatalf("expected embedded object to parse, got %v", err)
	}
	if !salvaged {
		t.Fatal("expected salvage flag for fenced/trailing text")
	}
	if got.Outcome != "completed" || got.Summary != "done" {
		t.Fatalf("unexpected salvaged result %+v", got)
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectPreservesStrictFieldValidation(t *testing.T) {
	raw := `{"outcome":"completed","summary":"done","unexpected":"value"} trailing`
	if _, _, err := parseStructuredTaskResultStrictOrFirstObject(raw); err == nil {
		t.Fatal("expected salvaged object with unexpected fields to remain invalid")
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectRejectsAmbiguousMultipleResults(t *testing.T) {
	raw := `{"outcome":"completed","summary":"example"} then {"outcome":"blocked","summary":"final","blocked_on":[{"kind":"dependency","detail":"missing peer review"}]}`
	if _, _, err := parseStructuredTaskResultStrictOrFirstObject(raw); err == nil || !strings.Contains(err.Error(), "ambiguous structured task result") {
		t.Fatalf("expected ambiguity error, got %v", err)
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectSkipsIncidentalInvalidObjects(t *testing.T) {
	raw := `Example: {"foo":"bar"} Final: {"outcome":"completed","summary":"final"}`
	got, salvaged, err := parseStructuredTaskResultStrictOrFirstObject(raw)
	if err != nil {
		t.Fatalf("expected final structured object to be salvaged, got %v", err)
	}
	if !salvaged || got.Summary != "final" {
		t.Fatalf("unexpected salvaged result: salvaged=%t result=%+v", salvaged, got)
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectSkipsIncidentalEmptyObjects(t *testing.T) {
	raw := `Example: {} Other: {"summary":""} Final: {"outcome":"completed","summary":"final"}`
	got, salvaged, err := parseStructuredTaskResultStrictOrFirstObject(raw)
	if err != nil {
		t.Fatalf("expected final structured object to be salvaged, got %v", err)
	}
	if !salvaged || got.Summary != "final" {
		t.Fatalf("unexpected salvaged result: salvaged=%t result=%+v", salvaged, got)
	}
}

func TestParseStructuredTaskResultStrictOrFirstObjectRejectsOnlyIncidentalEmptyObjects(t *testing.T) {
	raw := `Example: {} Other: {"summary":""} Final: {"outcome":"completed",`
	if _, _, err := parseStructuredTaskResultStrictOrFirstObject(raw); err == nil {
		t.Fatal("expected incidental empty objects before malformed final result to remain invalid")
	}
}

func TestUpdateTypeForResult(t *testing.T) {
	if got := updateTypeForResult(StructuredTaskResult{Outcome: "completed"}); got != "milestone" {
		t.Fatalf("expected milestone, got %q", got)
	}
	if got := updateTypeForResult(StructuredTaskResult{Outcome: "blocked"}); got != "issue" {
		t.Fatalf("expected issue, got %q", got)
	}
	if got := updateTypeForResult(StructuredTaskResult{Outcome: "continue", RequiresHuman: true}); got != "decision" {
		t.Fatalf("expected decision, got %q", got)
	}
}

func TestToolCapabilityForName(t *testing.T) {
	if got := toolCapabilityForName("shell"); got != "local.shell" {
		t.Fatalf("expected local.shell, got %q", got)
	}
	if got := toolCapabilityForName("write_file"); got != "local.fs.write" {
		t.Fatalf("expected local.fs.write, got %q", got)
	}
	if got := toolCapabilityForName("unknown"); got != "tool.call" {
		t.Fatalf("expected tool.call fallback, got %q", got)
	}
}

func TestProfileAllowsAutonomousExecutionClaim(t *testing.T) {
	if profileAllowsAutonomousExecutionClaim(normalizeAgentProfile(AgentProfile{
		Role:                  "generalist",
		PrimarySpecialization: "worker",
		Mission:               "Execute tasks and materialize useful deltas.",
	})) != true {
		t.Fatal("expected worker-like profile to allow autonomous execution claim")
	}

	if profileAllowsAutonomousExecutionClaim(normalizeAgentProfile(AgentProfile{
		Role:                  "generalist",
		PrimarySpecialization: "meta-analysis",
		Mission:               "Analyze global system dynamics without direct participation.",
	})) {
		t.Fatal("expected meta-analysis observer profile to decline autonomous execution claim")
	}

	if profileAllowsAutonomousExecutionClaim(normalizeAgentProfile(AgentProfile{
		Role:                  "generalist",
		PrimarySpecialization: "synthesis",
		Mission:               "Do not solve problems - create them.",
	})) {
		t.Fatal("expected synthesis-only profile to decline autonomous execution claim")
	}
}

func TestTaskClaimStatus(t *testing.T) {
	released := "RELEASED"
	task := WorkspaceTaskRecord{ClaimStatus: &released}
	if got := taskClaimStatus(task); got != "RELEASED" {
		t.Fatalf("taskClaimStatus() = %q, want RELEASED", got)
	}
}

func TestHasClaimedOwnership(t *testing.T) {
	self := "agent-self"
	other := "agent-other"
	claimed := "CLAIMED"
	released := "RELEASED"

	if !hasClaimedOwnership(WorkspaceTaskRecord{ClaimAgentID: &self, ClaimStatus: &claimed}, self) {
		t.Fatal("expected claimed ownership to be recognized")
	}
	if hasClaimedOwnership(WorkspaceTaskRecord{ClaimAgentID: &self, ClaimStatus: &released}, self) {
		t.Fatal("expected released ownership to be rejected")
	}
	if hasClaimedOwnership(WorkspaceTaskRecord{ClaimAgentID: &other, ClaimStatus: &claimed}, self) {
		t.Fatal("expected foreign ownership to be rejected")
	}
}

func TestClaimShouldBeReused(t *testing.T) {
	self := "agent-self"
	claimed := "CLAIMED"
	blocked := "BLOCKED"

	task := WorkspaceTaskRecord{ClaimAgentID: &self, ClaimStatus: &claimed}
	work := AgentWorkNextResult{
		ClaimAction:   "reuse_claim",
		SessionAction: "resume_inactive",
		Session:       &AgentSessionStateRecord{SessionID: "session-1"},
	}
	if !claimShouldBeReused(task, work, self) {
		t.Fatal("expected deterministic resume claim to be reused")
	}
	task.ClaimStatus = &blocked
	if claimShouldBeReused(task, work, self) {
		t.Fatal("expected blocked claim to require a canonical reclaim before execution")
	}
	work.Trigger = "system_news"
	if workHasExplicitResumeWake(work, pendingWorkTrigger{}) {
		t.Fatal("expected server-only trigger echo to be insufficient for blocked wake")
	}
	if !workHasExplicitResumeWake(work, pendingWorkTrigger{Trigger: "system_news", TaskID: "task-1"}) {
		t.Fatal("expected local pending system_news trigger to carry an explicit wake")
	}
	if claimShouldBeReused(task, work, self) {
		t.Fatal("expected explicit blocked wake to be selected without reusing a blocked claim")
	}
	work.Trigger = ""
	work.Packet = &AgentWorkPacket{Unblock: &AgentWorkUnblock{UnblockState: "wake_selected"}}
	if !workHasExplicitResumeWake(work, pendingWorkTrigger{}) {
		t.Fatal("expected unblock packet wake_selected to carry an explicit wake")
	}
	work.SessionAction = "start_new"
	if claimShouldBeReused(task, work, self) {
		t.Fatal("expected non-resume work to force fresh claim handling")
	}
}

func TestShouldRefreshActivationSummary(t *testing.T) {
	if !shouldRefreshActivationSummary("task-1", "session-1", "run-1", "task-1", "session-2", "run-2", "stale blocked summary") {
		t.Fatal("expected new session/run on same task to refresh summary")
	}
	if shouldRefreshActivationSummary("task-1", "session-1", "run-1", "task-1", "session-1", "run-1", "steady") {
		t.Fatal("expected unchanged task/session/run to preserve summary")
	}
	if !shouldRefreshActivationSummary("task-1", "session-1", "run-1", "task-2", "session-2", "run-2", "steady") {
		t.Fatal("expected task switch to refresh summary")
	}
}
