package main

import "testing"

func TestTaskCycleInitialPhaseRecordsBindSelectionClaimSessionRun(t *testing.T) {
	claimedBy := "agent-1"
	claimStatus := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Do the work",
		Description:  "Make progress",
		Status:       "RUNNING",
		ClaimAgentID: &claimedBy,
		ClaimStatus:  &claimStatus,
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-real",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}
	work := &AgentWorkPacket{
		WorkType:            "resume_session",
		ClaimAction:         "reuse_claim",
		SessionAction:       "resume_inactive",
		PreferredTransition: "continue",
		WhyNow:              "existing session still active",
	}

	records := taskCycleInitialPhaseRecords(cfg, task, session, "run-real", work)
	if len(records) != 4 {
		t.Fatalf("expected 4 initial phase records, got %d", len(records))
	}
	wantPhases := []string{"WORK_SELECTED", "CLAIM_REUSED", "SESSION_BOUND", "RUN_BOUND"}
	for i, want := range wantPhases {
		if got := stringMapField(t, records[i], "phase_name"); got != want {
			t.Fatalf("phase %d = %q, want %q", i, got, want)
		}
		if got := intMapField(t, records[i], "phase_sequence"); got != i+1 {
			t.Fatalf("phase %s sequence = %d, want %d", want, got, i+1)
		}
		if got := stringMapField(t, records[i], "task_id"); got != task.TaskID {
			t.Fatalf("phase %s task_id = %q, want %q", want, got, task.TaskID)
		}
	}

	claim := nestedMapField(t, records[1], "claim")
	if got := stringMapField(t, claim, "claim_result"); got != "reused" {
		t.Fatalf("claim_result = %q, want reused", got)
	}
	if got := stringMapField(t, claim, "claim_status_at_start"); got != "CLAIMED" {
		t.Fatalf("claim_status_at_start = %q, want CLAIMED", got)
	}
	if got := stringMapField(t, claim, "claimed_by_agent_id"); got != "agent-1" {
		t.Fatalf("claimed_by_agent_id = %q, want agent-1", got)
	}

	sessionBinding := nestedMapField(t, records[2], "session")
	if got := stringMapField(t, sessionBinding, "session_id"); got != "session-real" {
		t.Fatalf("session_id = %q, want session-real", got)
	}
	if got := stringMapField(t, sessionBinding, "session_status_at_start"); got != "ACTIVE" {
		t.Fatalf("session_status_at_start = %q, want ACTIVE", got)
	}

	runBinding := nestedMapField(t, records[3], "run")
	if got := stringMapField(t, runBinding, "run_id"); got != "run-real" {
		t.Fatalf("run_id = %q, want run-real", got)
	}
	if got := stringMapField(t, runBinding, "run_status_at_start"); got != "ACTIVE" {
		t.Fatalf("run_status_at_start = %q, want ACTIVE", got)
	}

	last := lastTaskCyclePhaseRecord(records)
	if got := stringMapField(t, last, "phase_name"); got != "RUN_BOUND" {
		t.Fatalf("last phase = %q, want RUN_BOUND", got)
	}
	basis := nestedMapField(t, last, "basis")
	if got := stringMapField(t, basis, "capability_snapshot_id"); got != daemonRunCapabilitySnapshotID(cfg, task, session, "run-real") {
		t.Fatalf("capability_snapshot_id = %q, want run snapshot id", got)
	}
	if got := stringMapField(t, basis, "objective_hash"); got == "" {
		t.Fatal("expected non-empty objective_hash")
	}
	if got := stringMapField(t, basis, "authority_term_status"); got != "unavailable_to_agent" {
		t.Fatalf("authority_term_status = %q, want unavailable_to_agent", got)
	}
}

func TestTaskCycleInitialPhaseRecordsMarkFreshClaimAcquired(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-2",
		Title:       "Fresh task",
		Description: "Start work",
		Status:      "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-new",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}

	records := taskCycleInitialPhaseRecords(cfg, task, session, "run-new", &AgentWorkPacket{
		WorkType:      "resume_session",
		ClaimAction:   "claim_required",
		SessionAction: "start_new",
	})
	if got := stringMapField(t, records[1], "phase_name"); got != "CLAIM_ACQUIRED" {
		t.Fatalf("claim phase = %q, want CLAIM_ACQUIRED", got)
	}
	claim := nestedMapField(t, records[1], "claim")
	if got := stringMapField(t, claim, "claim_result"); got != "acquired" {
		t.Fatalf("claim_result = %q, want acquired", got)
	}
}

func TestTaskCycleInitialPhaseRecordsDoNotTreatStartNewAsClaimReuse(t *testing.T) {
	claimedBy := "agent-1"
	claimStatus := "PAUSED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-3",
		Title:        "Paused owned task",
		Status:       "PAUSED",
		ClaimAgentID: &claimedBy,
		ClaimStatus:  &claimStatus,
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-new",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}

	records := taskCycleInitialPhaseRecords(cfg, task, session, "run-new", &AgentWorkPacket{
		WorkType:      "resume_claim",
		ClaimAction:   "reuse_claim",
		SessionAction: "start_new",
	})
	if got := stringMapField(t, records[1], "phase_name"); got != "CLAIM_ACQUIRED" {
		t.Fatalf("claim phase = %q, want CLAIM_ACQUIRED", got)
	}
	claim := nestedMapField(t, records[1], "claim")
	if got := stringMapField(t, claim, "claim_result"); got != "acquired" {
		t.Fatalf("claim_result = %q, want acquired", got)
	}
}

func TestTaskCycleInitialPhaseRecordsCanReuseLiveClaimWithNewSession(t *testing.T) {
	claimedBy := "agent-1"
	claimStatus := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-live",
		Title:        "Live owned task",
		Status:       "RUNNING",
		ClaimAgentID: &claimedBy,
		ClaimStatus:  &claimStatus,
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-new",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}

	records := taskCycleInitialPhaseRecords(cfg, task, session, "run-new", &AgentWorkPacket{
		WorkType:      "bootstrap_scan",
		ClaimAction:   "reuse_claim",
		SessionAction: "start_new",
	})
	if got := stringMapField(t, records[1], "phase_name"); got != "CLAIM_REUSED" {
		t.Fatalf("claim phase = %q, want CLAIM_REUSED", got)
	}
	claim := nestedMapField(t, records[1], "claim")
	if got := stringMapField(t, claim, "claim_result"); got != "reused" {
		t.Fatalf("claim_result = %q, want reused", got)
	}
}

func TestTaskCycleProgressPhaseRecordsCaptureCheckpointAndProgress(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-progress",
		Title:       "Progress task",
		Description: "Keep moving",
		Status:      "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-progress",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Need a follow-up",
		Materialize: TaskMaterialization{
			DocKey:     "task.progress",
			DocTitle:   "Progress",
			DocContent: "details",
		},
	}
	trace := &TaskRunTrace{ToolCalls: []string{"workspace.doc.put"}}

	records := taskCycleProgressPhaseRecords(cfg, task, session, "run-progress", nil, &result, trace)
	if len(records) != 2 {
		t.Fatalf("expected 2 progress phase records, got %d", len(records))
	}
	if got := stringMapField(t, records[0], "phase_name"); got != "CHECKPOINT_SAVED" {
		t.Fatalf("checkpoint phase = %q, want CHECKPOINT_SAVED", got)
	}
	if got := stringMapField(t, records[1], "phase_name"); got != "PROGRESS_RECORDED" {
		t.Fatalf("progress phase = %q, want PROGRESS_RECORDED", got)
	}
	if got := intMapField(t, records[0], "phase_sequence"); got != 5 {
		t.Fatalf("checkpoint sequence = %d, want 5", got)
	}
	if got := intMapField(t, records[1], "phase_sequence"); got != 6 {
		t.Fatalf("progress sequence = %d, want 6", got)
	}
	checkpoint := nestedMapField(t, records[0], "checkpoint")
	if got := stringMapField(t, checkpoint, "checkpoint_id"); got == "" {
		t.Fatal("expected checkpoint_id to be populated")
	}
	if got := stringMapField(t, checkpoint, "last_durable_step"); got != "CHECKPOINT_SAVED" {
		t.Fatalf("checkpoint last_durable_step = %q, want CHECKPOINT_SAVED", got)
	}
}

func TestTaskCycleTerminalPhaseRecordsCaptureEvidenceRefs(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-terminal",
		Title:       "Terminal task",
		Description: "Finish cleanly",
		Status:      "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-terminal",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Done",
		Materialize: TaskMaterialization{
			DocKey:     "task.final",
			DocTitle:   "Final",
			DocContent: "final",
		},
	}

	records := taskCycleTerminalPhaseRecords(cfg, task, session, "run-terminal", nil, &result, nil)
	if len(records) != 2 {
		t.Fatalf("expected 2 terminal phase records, got %d", len(records))
	}
	if got := stringMapField(t, records[0], "phase_name"); got != "RESULT_RECORDED" {
		t.Fatalf("result phase = %q, want RESULT_RECORDED", got)
	}
	if got := stringMapField(t, records[1], "phase_name"); got != "TERMINAL_TRANSITIONED" {
		t.Fatalf("terminal phase = %q, want TERMINAL_TRANSITIONED", got)
	}
	if got := intMapField(t, records[0], "phase_sequence"); got != 7 {
		t.Fatalf("result sequence = %d, want 7", got)
	}
	if got := intMapField(t, records[1], "phase_sequence"); got != 8 {
		t.Fatalf("terminal sequence = %d, want 8", got)
	}
	resultRecord := nestedMapField(t, records[0], "structured_result")
	evidenceRefs := stringsSliceField(t, resultRecord, "evidence_refs")
	if len(evidenceRefs) != 1 || evidenceRefs[0] != "doc:task.final" {
		t.Fatalf("unexpected result evidence refs: %+v", evidenceRefs)
	}
	terminal := nestedMapField(t, records[1], "terminal_transition")
	terminalEvidenceRefs := stringsSliceField(t, terminal, "terminal_evidence_refs")
	if len(terminalEvidenceRefs) != 1 || terminalEvidenceRefs[0] != "doc:task.final" {
		t.Fatalf("unexpected terminal evidence refs: %+v", terminalEvidenceRefs)
	}
	ops := nestedMapField(t, records[1], "operations")
	if got := stringsSliceField(t, ops, "operation_ids"); len(got) != 0 {
		t.Fatalf("expected no operation ids, got %+v", got)
	}
}

func TestTaskCycleTerminalPhaseRecordsCanMarkSuppressedCanonicalMutation(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-fenced",
		Title:  "Fenced completion",
		Status: "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-fenced",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Late result should not mutate canonical task state",
	}
	work := &AgentWorkPacket{WorkType: "resume_session", ClaimAction: "reuse_claim", SessionAction: "resume_inactive"}

	records := taskCycleTerminalPhaseRecords(cfg, task, session, "run-fenced", work, &result, nil)
	markTerminalCanonicalMutationApplied(records, false)

	terminal := nestedMapField(t, records[1], "terminal_transition")
	if got := boolMapField(t, terminal, "canonical_mutation_applied"); got {
		t.Fatalf("expected late fenced terminal record to mark canonical_mutation_applied=false, got %+v", terminal)
	}
	if got := stringMapField(t, records[1], "selection_idempotency_key"); got != "resume_session" {
		t.Fatalf("expected terminal record to retain frozen work packet selection key, got %q", got)
	}
}

func TestTaskCycleInitialPhaseRecordsDoNotInferReuseFromNilPacket(t *testing.T) {
	claimedBy := "agent-1"
	claimStatus := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-4",
		Title:        "Claimed task without packet",
		Status:       "RUNNING",
		ClaimAgentID: &claimedBy,
		ClaimStatus:  &claimStatus,
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-bootstrap",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}

	records := taskCycleInitialPhaseRecords(cfg, task, session, "run-bootstrap", nil)
	if got := stringMapField(t, records[1], "phase_name"); got != "CLAIM_ACQUIRED" {
		t.Fatalf("claim phase = %q, want CLAIM_ACQUIRED", got)
	}
	claim := nestedMapField(t, records[1], "claim")
	if got := stringMapField(t, claim, "claim_result"); got != "acquired" {
		t.Fatalf("claim_result = %q, want acquired", got)
	}
}

func stringMapField(t *testing.T, values map[string]any, key string) string {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing field %q in %+v", key, values)
	}
	got, ok := raw.(string)
	if !ok {
		t.Fatalf("field %q has type %T, want string", key, raw)
	}
	return got
}

func intMapField(t *testing.T, values map[string]any, key string) int {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing field %q in %+v", key, values)
	}
	got, ok := raw.(int)
	if !ok {
		t.Fatalf("field %q has type %T, want int", key, raw)
	}
	return got
}

func nestedMapField(t *testing.T, values map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing nested field %q in %+v", key, values)
	}
	got, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("nested field %q has type %T, want map[string]any", key, raw)
	}
	return got
}

func stringsSliceField(t *testing.T, values map[string]any, key string) []string {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing slice field %q in %+v", key, values)
	}
	got, ok := raw.([]string)
	if !ok {
		t.Fatalf("field %q has type %T, want []string", key, raw)
	}
	return got
}

func boolMapField(t *testing.T, values map[string]any, key string) bool {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing bool field %q in %+v", key, values)
	}
	got, ok := raw.(bool)
	if !ok {
		t.Fatalf("field %q has type %T, want bool", key, raw)
	}
	return got
}
