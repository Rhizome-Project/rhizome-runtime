package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTaskAllowsBlockedTransitionRequiresOwnedClaimedOrBlockedTask(t *testing.T) {
	self := "agent-self"
	other := "agent-other"
	claimed := "CLAIMED"
	blocked := "BLOCKED"
	released := "RELEASED"

	tests := []struct {
		name string
		task WorkspaceTaskRecord
		want bool
	}{
		{
			name: "owned claimed running task stays blockable",
			task: WorkspaceTaskRecord{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
			want: true,
		},
		{
			name: "owned blocked running task can still write terminal blocked receipt",
			task: WorkspaceTaskRecord{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &blocked},
			want: true,
		},
		{
			name: "released ownership is not blockable",
			task: WorkspaceTaskRecord{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &released},
			want: false,
		},
		{
			name: "foreign ownership is not blockable",
			task: WorkspaceTaskRecord{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &other, ClaimStatus: &claimed},
			want: false,
		},
		{
			name: "cancelled task is not blockable",
			task: WorkspaceTaskRecord{TaskID: "task-1", Status: "CANCELLED", ClaimAgentID: &self, ClaimStatus: &claimed},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := taskAllowsBlockedTransition(tt.task, self); got != tt.want {
				t.Fatalf("taskAllowsBlockedTransition() = %v, want %v", got, tt.want)
			}
			if strings.EqualFold(taskClaimStatus(tt.task), "BLOCKED") && taskAllowsCompletionTransition(tt.task, self) {
				t.Fatalf("same-owner BLOCKED claim should be blockable for terminal receipt but not completable")
			}
		})
	}
}

func TestFailedPatchQueueSupersedeStewardshipCompletesInsteadOfReleasing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	taskID := "task-patchq-supersede-project-abc"
	methods := []string{}
	taskCompleted := false
	sessionEnded := false
	savedStates := []RuntimeScratchState{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: taskID, Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "agent.session.blocked":
			t.Fatalf("deterministic supersede parking must end the session instead of leaving a blocked active scratch loop: %+v", req.Params)
		case "agent.task.block":
			t.Fatalf("failed patch queue supersede stewardship should close the current stale task, not leave a reclaimable block: %+v", req.Params)
		case "agent.task.complete":
			taskCompleted = true
			if got := rpcString(req.Params, "task_id"); got != taskID {
				t.Fatalf("completed wrong task: %+v", req.Params)
			}
			if !strings.Contains(rpcString(req.Params, "summary"), "evidence_doc_key") {
				t.Fatalf("expected deterministic supersede failure summary, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"task_id": taskID, "status": "RESOLVED"})
		case "agent.task.release":
			t.Fatalf("failed patch queue supersede stewardship must not return to shared retry pool: %+v", req.Params)
		case "workspace.execution.run.write":
			if got := rpcString(req.Params, "status"); got != "COMPLETED" {
				t.Fatalf("expected COMPLETED run status, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.execution.step.write":
			if got := rpcString(req.Params, "title"); got != "Verify completion" {
				t.Fatalf("expected completion terminal step, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.tension.refresh":
			writeRPCResult(w, req, map[string]any{"workspace_id": "ws-1", "refresh": map[string]any{}})
		case "agent.session.end":
			sessionEnded = true
			if got := rpcString(req.Params, "status"); got != "ENDED" {
				t.Fatalf("expected ENDED session status, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:   rpcString(req.Params, "session_id"),
					WorkspaceID: rpcString(req.Params, "workspace_id"),
					AgentID:     rpcString(req.Params, "agent_id"),
					TaskID:      rpcString(req.Params, "task_id"),
					Status:      "ENDED",
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err == nil {
				savedStates = append(savedStates, state)
			}
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:01Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-05-13T00:00:00Z",
					"updated_at":       "2026-05-13T00:00:01Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during failed supersede parking: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       taskID,
		Title:        "Supersede blocked patch queue item after fresh evidence",
		Description:  "project_patch_queue_lifecycle action=supersede for blocked patch queue stewardship",
		Status:       "RUNNING",
		Tags:         []string{"queue-stewardship", "supersede"},
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      taskID,
		Status:      "ACTIVE",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    taskID,
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	err := runtime.failedTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome: "failed",
		Summary: "project_patch_queue_lifecycle supersede failed because evidence_doc_key describes missing/blocked browser-smoke validation",
	}, nil, nil)
	if err != nil {
		t.Fatalf("failedTaskCycle() error = %v", err)
	}
	if !taskCompleted {
		t.Fatalf("expected failed supersede stewardship to complete the stale task, methods=%v", methods)
	}
	if !sessionEnded {
		t.Fatalf("expected failed supersede stewardship to end the session, methods=%v", methods)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected scratch save after supersede parking, methods=%v", methods)
	}
	last := savedStates[len(savedStates)-1]
	if last.ActiveTaskID != "" || last.ActiveSessionID != "" || last.ActiveRunID != "" {
		t.Fatalf("expected parked supersede task to clear active scratch, got %+v", last)
	}
}

func TestPatchQueueSupersedeToolFailureOverridesContinueOutcome(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-supersede-project-abc",
		Title:       "Supersede blocked patch queue item after fresh evidence",
		Description: "project_patch_queue_lifecycle action=supersede for blocked patch queue stewardship",
		Status:      "RUNNING",
		Tags:        []string{"queue-stewardship", "supersede"},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Will inspect the same patch queue supersede again later",
	}
	trace := &TaskRunTrace{FailedToolCalls: []string{"project_patch_queue_lifecycle"}}
	if !patchQueueSupersedeStewardshipToolFailed(task, result, trace) {
		t.Fatal("expected failed project_patch_queue_lifecycle supersede stewardship tool call to force parking")
	}
	parked := blockedPatchQueueSupersedeStewardshipResult(result)
	if normalizeOutcome(parked.Outcome) != "blocked" || shouldSurfaceOperatorQueue(parked) {
		t.Fatalf("expected non-human blocked parking result, got %+v", parked)
	}
	if !shouldEndRoutineDependencyBlockedSession(parked) {
		t.Fatalf("expected parked supersede blocker to end session and clear active scratch, got %+v", parked.BlockedOn)
	}
	if completed, ok := routineBlockedResultAsCompletedTask(task, parked); !ok || normalizeOutcome(completed.Outcome) != "completed" {
		t.Fatalf("expected parked supersede stewardship to close current task as completed, got ok=%v result=%+v", ok, completed)
	}
}

func TestPatchQueueSupersedeInvalidEvidenceObservationOverridesContinueOutcome(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-patchq-supersede-project-abc",
		Title:  "Supersede blocked patch queue item after fresh evidence",
		Description: `project_patch_queue_lifecycle action=supersede
evidence_doc_key: agent.zeta.claimed_work`,
		Status: "RUNNING",
		Tags:   []string{"queue-stewardship", "supersede"},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: "Inspected evidence_doc_key agent.zeta.claimed_work.",
		Details: "It is a Claimed Work Ledger with active_claimed_work and is not validation evidence; it does not bind exact branch/head, so cannot call project_patch_queue_lifecycle action=supersede.",
	}
	if !patchQueueSupersedeStewardshipInvalidEvidenceObserved(task, result) {
		t.Fatal("expected invalid agent-state evidence observation to force supersede stewardship parking")
	}
	parked := blockedPatchQueueSupersedeStewardshipResult(result)
	if normalizeOutcome(parked.Outcome) != "blocked" || shouldSurfaceOperatorQueue(parked) {
		t.Fatalf("expected non-human blocked parking result, got %+v", parked)
	}
	if !shouldEndRoutineDependencyBlockedSession(parked) {
		t.Fatalf("expected invalid supersede evidence parking to end session and clear active scratch, got %+v", parked.BlockedOn)
	}
	if completed, ok := routineBlockedResultAsCompletedTask(task, parked); !ok || normalizeOutcome(completed.Outcome) != "completed" {
		t.Fatalf("expected invalid supersede stewardship to close current task as completed, got ok=%v result=%+v", ok, completed)
	}
}

func TestPatchQueueSupersedePreflightBlocksAgentStateEvidenceKey(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-patchq-supersede-project-abc",
		Title:  "Supersede blocked patch queue item after fresh evidence",
		Description: `Patch queue stewardship task created from agent.work.next frontier project_patch_queue_supersede_available.

project_patch_queue_lifecycle args:
- action: supersede
- evidence_doc_key: agent.beta.current_context
- branch_id: branch-1`,
		Status: "RUNNING",
		Tags:   []string{"queue-stewardship", "supersede"},
	}
	result, ok := patchQueueSupersedeStewardshipPreflightBlockResult(task)
	if !ok {
		t.Fatal("expected stale agent-state evidence key to preflight block supersede stewardship")
	}
	if normalizeOutcome(result.Outcome) != "blocked" || shouldSurfaceOperatorQueue(result) {
		t.Fatalf("expected non-human blocked preflight result, got %+v", result)
	}
	if !strings.Contains(result.Summary, "agent.beta.current_context") {
		t.Fatalf("preflight summary should name invalid evidence doc key, got %q", result.Summary)
	}
}

func TestPatchQueueSupersedePreflightAllowsTaskEvidenceKey(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID: "task-patchq-supersede-project-abc",
		Title:  "Supersede blocked patch queue item after fresh evidence",
		Description: `Patch queue stewardship task created from agent.work.next frontier project_patch_queue_supersede_available.

project_patch_queue_lifecycle args:
- action: supersede
- evidence_doc_key: task.task-visual.visual_acceptance
- branch_id: branch-1`,
		Status: "RUNNING",
		Tags:   []string{"queue-stewardship", "supersede"},
	}
	if _, ok := patchQueueSupersedeStewardshipPreflightBlockResult(task); ok {
		t.Fatal("task-scoped evidence doc key must not be preflight blocked")
	}
}

func TestPatchQueueSupersedeNegativeVisualVerdictObservationOverridesContinueOutcome(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-supersede-project-abc",
		Title:       "Supersede blocked patch queue item after fresh evidence",
		Description: `project_patch_queue_lifecycle action=supersede with evidence_doc_key task.visual_acceptance`,
		Status:      "RUNNING",
		Tags:        []string{"queue-stewardship", "supersede"},
	}
	result := StructuredTaskResult{
		Outcome: "continue",
		Summary: `Reviewed evidence_doc_key task.visual_acceptance; {"schema":"rhizome_visual_acceptance_v1","visual_verdict":"block"}`,
		Details: "The screenshot capture result: pass only proves capture, while the visual verdict remains blocked.",
	}
	if !patchQueueSupersedeStewardshipInvalidEvidenceObserved(task, result) {
		t.Fatal("expected explicit negative visual verdict to force supersede stewardship parking")
	}
}

func TestVisualFailureRevisionFollowupArgsFromBlockedPacket(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:    "task-visual",
		ProjectID: "project-minesweeper",
	}
	result := StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    "Published visual packet with visual_verdict: fail",
		NextAction: "Use the blocking findings as input for the next implementation/revision lane.",
		Materialize: TaskMaterialization{
			DocKey:   "task.task-visual.visual_acceptance",
			DocTitle: "Visual Acceptance Packet",
			DocContent: `# Visual Acceptance Packet

- schema_label: rhizome_visual_acceptance_v1
- project_id: project-minesweeper
- queue_id: patchq-project-minesweeper-repo
- item_id: patchitem-branch-1
- branch_id: branch-1
- head_sha: c315d695ea9c03267c1de953c58c27649e520a20

## Narrow Viewport Judgment
- First viewport: fail. The first mobile viewport is dominated by header/control chrome before the primary board.
- Primary-surface geometry: the board is pushed too far down and loses visual dominance.

## Smallest Repair Direction
Reduce top-of-screen chrome and promote the board upward on narrow viewports.

## Final Verdict
- visual_verdict: fail`,
		},
	}
	args, ok := visualFailureRevisionFollowupArgs(task, result)
	if !ok {
		t.Fatal("expected visual fail packet to request revision follow-up")
	}
	if args["project_id"] != "project-minesweeper" || args["queue_id"] != "patchq-project-minesweeper-repo" || args["item_id"] != "patchitem-branch-1" || args["followup_kind"] != "revision" {
		t.Fatalf("unexpected followup args: %+v", args)
	}
	extra := args["extra_context"].(string)
	for _, want := range []string{"visual_failure_doc_key: task.task-visual.visual_acceptance", "visual_failure_branch_id: branch-1", "visual_failure_head_sha: c315d695ea9c03267c1de953c58c27649e520a20"} {
		if !strings.Contains(extra, want) {
			t.Fatalf("extra context missing %q:\n%s", want, extra)
		}
	}
}

func TestVisualFailureRevisionFollowupArgsAcceptsSchemaVerdictAlias(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-visual-review"}
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Published rhizome_visual_acceptance_v1 packet with verdict fail",
		Materialize: TaskMaterialization{
			DocKey:   "task.task-visual-review.visual_acceptance",
			DocTitle: "Visual Acceptance - candidate",
			DocContent: `schema: rhizome_visual_acceptance_v1
project_id: project-minesweeper
task_id: task-visual-review
patch_item_id: patchitem-projbranch-1-r2
candidate_head_sha: 322f2bb292a08e4f38bf881986cdaf5c297125ac
verdict: fail
verdict_scope: blocking
core_user_promise_check: The candidate is a real playable Minesweeper surface, but it fails the board-first responsive promise on narrow viewport and therefore does not meet visual acceptance.
viewports:
  - id: narrow
    first_viewport_judgment: fail
    findings:
      - Responsive layout breaks on narrow viewport.
      - Horizontal document scroll expands beyond the viewport for every visible difficulty.
primary_surface_geometry_density:
  intermediate:
    narrow: fail; board width exceeds viewport width.
smallest_repair_direction: Stack the mobile layout into a true single-column board-first flow, constrain panel widths to the viewport, and introduce narrow-specific board sizing.
final_judgment: Do not accept this candidate into visual acceptance.`,
		},
	}
	args, ok := visualFailureRevisionFollowupArgs(task, result)
	if !ok {
		t.Fatal("expected verdict: fail visual packet to request revision follow-up")
	}
	if args["project_id"] != "project-minesweeper" || args["item_id"] != "patchitem-projbranch-1-r2" || args["followup_kind"] != "revision" {
		t.Fatalf("unexpected followup args: %+v", args)
	}
	extra := args["extra_context"].(string)
	if !strings.Contains(extra, "visual_failure_head_sha: 322f2bb292a08e4f38bf881986cdaf5c297125ac") {
		t.Fatalf("extra context missing candidate head sha:\n%s", extra)
	}
}

func TestVisualFailureRevisionFollowupArgsSkipsEvidenceOnlyPacket(t *testing.T) {
	result := StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Visual packet incomplete",
		Materialize: TaskMaterialization{
			DocKey: "task.visual_acceptance",
			DocContent: `schema: rhizome_visual_acceptance_v1
project_id: project-minesweeper
queue_id: patchq-project-minesweeper-repo
item_id: patchitem-branch-1
visual_verdict: fail
smallest_repair_direction: Publish a complete rhizome_visual_acceptance_v1 packet for this same committed head with semantic desktop+narrow screenshot judgment plus primary interaction and result-state evidence.`,
		},
	}
	if args, ok := visualFailureRevisionFollowupArgs(WorkspaceTaskRecord{ProjectID: "project-minesweeper"}, result); ok {
		t.Fatalf("evidence-only visual packet should not create implementation revision args: %+v", args)
	}
}

func TestCompletedVisualFailRequestsRevisionFollowup(t *testing.T) {
	self := "agent-kappa"
	claimed := "CLAIMED"
	taskID := "task-visual-review"
	calls := []string{}
	listTasksCalls := 0
	taskSubmitted := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			listTasksCalls++
			if listTasksCalls == 1 {
				writeRPCResult(w, req, map[string]any{"tasks": []WorkspaceTaskRecord{{
					TaskID:       taskID,
					Status:       "RUNNING",
					ClaimAgentID: &self,
					ClaimStatus:  &claimed,
				}}})
				return
			}
			writeRPCResult(w, req, map[string]any{"tasks": []WorkspaceTaskRecord{}})
		case "agent.task.complete":
			writeRPCResult(w, req, nil)
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{DocKey: rpcString(req.Params, "doc_key"), SHA: "remote-sha"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "workspace.artifact.write":
			writeRPCResult(w, req, map[string]any{"artifact": WorkspaceArtifactRecord{
				ArtifactID:  "artifact-1",
				ArtifactRef: rpcString(req.Params, "artifact_ref"),
			}})
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state": "BLOCKED",
				"decision_summary": strings.Join([]string{
					"Visual acceptance packet says visual_verdict: fail.",
					"Responsive layout breaks on narrow viewport and horizontal overflow pushes the board past the viewport.",
					"Smallest repair direction: constrain panel widths and repair the responsive board layout.",
				}, " "),
				"decision_doc_key": "task.task-visual-review.visual_acceptance",
			}))
		case "task.submit":
			taskSubmitted = true
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if description := rpcString(req.Params, "description"); !strings.Contains(description, "visual_failure_doc_key: task.task-visual-review.visual_acceptance") {
				t.Fatalf("revision followup missing visual failure context:\n%s", description)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws-1", "status": "PENDING"})
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.session.end":
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   rpcString(req.Params, "session_id"),
				WorkspaceID: rpcString(req.Params, "workspace_id"),
				AgentID:     rpcString(req.Params, "agent_id"),
				TaskID:      rpcString(req.Params, "task_id"),
				Status:      "ENDED",
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:01Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Kappa",
					"role":             "visual verifier",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-05-13T00:00:00Z",
					"updated_at":       "2026-05-13T00:00:01Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected RPC method %s; calls=%v", req.Method, calls)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       taskID,
		Title:        "Visual acceptance validation for Minesweeper candidate",
		ProjectID:    "project-subpixel",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-visual",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      taskID,
		Status:      "ACTIVE",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
			OwnerUserID:  "owner-1",
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-visual",
		scratch: RuntimeScratchState{
			ActiveTaskID:    taskID,
			ActiveSessionID: "session-visual",
			ActiveRunID:     "run-visual",
			DocSHAs:         map[string]string{},
		},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Published rhizome_visual_acceptance_v1 packet with visual_verdict: fail for narrow responsive layout.",
		Materialize: TaskMaterialization{
			DocKey:   "task.task-visual-review.visual_acceptance",
			DocTitle: "Visual Acceptance - candidate",
			DocContent: `schema: rhizome_visual_acceptance_v1
project_id: project-subpixel
queue_id: patchq-project-subpixel-projrepo-1
item_id: patchitem-branch-1
branch_id: branch-1
head_sha: 322f2bb292a08e4f38bf881986cdaf5c297125ac
visual_verdict: fail
verdict_scope: blocking
findings:
  - Responsive layout breaks on narrow viewport.
  - Horizontal overflow pushes the board wide past the viewport.
smallest_repair_direction: Repair the responsive board layout and constrain panel widths to the viewport.`,
		},
	}
	if err := runtime.completeTaskCycle(context.Background(), task, session, "run-visual", result, nil, nil); err != nil {
		t.Fatalf("completeTaskCycle() error = %v", err)
	}
	if !taskSubmitted {
		t.Fatalf("expected completed visual fail to submit a revision followup; calls=%v", calls)
	}
}

func TestBlockTaskCycleSuppressesStaleBlockedTransitionAfterOwnershipDrift(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	other := "agent-other"
	claimed := "CLAIMED"
	methods := []string{}
	savedStates := []RuntimeScratchState{}
	claimedWorkDocWrites := 0
	currentContextDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &other, ClaimStatus: &claimed},
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey("agent-1"):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected cleared current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-context-cleared"})
			case claimedWorkDocKey("agent-1"):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected cleared claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-cleared"})
			default:
				t.Fatalf("unexpected doc key: %+v", req.Params)
			}
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-18T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-18T00:00:00Z",
					"updated_at":       "2026-04-18T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during stale blocked transition suppression: %s", req.Method)
		}
	}))
	defer server.Close()

	self := "agent-1"
	taskClaimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &taskClaimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-1",
		TaskID:    "task-1",
		Status:    "ACTIVE",
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.blockTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Need human intervention",
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: "needs review"}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("blockTaskCycle() error: %v", err)
	}

	expectedMethods := []string{
		"workspace.tasks.list",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"workspace.doc.put",
		"agent.state.set",
		"agent.bootstrap",
		"agent.limits.get",
		"agent.state.set",
	}
	if len(methods) != len(expectedMethods) {
		t.Fatalf("unexpected method count: got %+v want %+v", methods, expectedMethods)
	}
	for i := range expectedMethods {
		if methods[i] != expectedMethods[i] {
			t.Fatalf("unexpected method order: got %+v want %+v", methods, expectedMethods)
		}
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected one current-context and one claimed-work reset, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	if len(savedStates) != 4 {
		t.Fatalf("expected four scratch state writes, got %d", len(savedStates))
	}
	if savedStates[0].ActiveTaskID != "" || savedStates[0].ActiveSessionID != "" || savedStates[0].ActiveRunID != "" {
		t.Fatalf("expected stale drift clear to drop active state, got %+v", savedStates[0])
	}
	if !strings.Contains(savedStates[0].LastSummary, "ownership drift") {
		t.Fatalf("expected drift summary in cleared scratch state, got %+v", savedStates[0])
	}
	if savedStates[1].DocSHAs[agentContextDocKey("agent-1")] != "sha-context-cleared" {
		t.Fatalf("expected current context doc sha to be persisted after reset, got %+v", savedStates[1])
	}
	if savedStates[2].DocSHAs[claimedWorkDocKey("agent-1")] != "sha-claimed-cleared" {
		t.Fatalf("expected claimed work doc sha to be persisted after reset, got %+v", savedStates[2])
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state to be cleared, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestBlockTaskCycleParksTaskWhenSessionAlreadyInactive(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	methods := []string{}
	savedStates := []RuntimeScratchState{}
	taskBlocked := false
	currentContextDocWrites := 0
	claimedWorkDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "agent.session.blocked":
			writeRPCError(w, req, -32000, "session is not active: session session-1 (ENDED)")
		case "agent.task.block":
			taskBlocked = true
			if got := rpcString(req.Params, "task_id"); got != "task-1" {
				t.Fatalf("blocked wrong task: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-1", "status": "BLOCKED"})
		case "workspace.tension.refresh":
			writeRPCResult(w, req, map[string]any{"workspace_id": "ws-1", "refresh": map[string]any{}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			switch rpcString(req.Params, "doc_key") {
			case agentContextDocKey(self):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected cleared current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-context-cleared"})
			case claimedWorkDocKey(self):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected cleared claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-cleared"})
			default:
				t.Fatalf("unexpected doc key: %+v", req.Params)
			}
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-28T02:00:01Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-28T02:00:00Z",
					"updated_at":       "2026-04-28T02:00:01Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks": []WorkspaceTaskRecord{
						{TaskID: "task-1", Title: "Task One", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: stringPtr("BLOCKED")},
					},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during inactive-session parking: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      "task-1",
		Status:      "ACTIVE",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.blockTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Execution failed before terminal block",
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: "session already ended"}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("blockTaskCycle() error: %v", err)
	}
	if !taskBlocked {
		t.Fatalf("expected inactive session path to park task claim, methods=%v", methods)
	}
	trace := strings.Join(methods, ",")
	for _, forbidden := range []string{"workspace.execution.run.write", "workspace.execution.step.write"} {
		if strings.Contains(trace, forbidden) {
			t.Fatalf("inactive session parking should not write terminal run evidence via ended session, got %v", methods)
		}
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected scratch state to be cleared, methods=%v", methods)
	}
	if savedStates[0].ActiveTaskID != "" || savedStates[0].ActiveSessionID != "" || savedStates[0].ActiveRunID != "" {
		t.Fatalf("expected inactive session parking to clear active scratch, got %+v", savedStates[0])
	}
	if !strings.Contains(savedStates[0].LastSummary, "inactive session") {
		t.Fatalf("expected inactive-session summary in scratch, got %+v", savedStates[0])
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected presence docs to clear once, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state to be cleared, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestBlockTaskCycleEndsRoutineDependencyBlockWithoutSessionQueue(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	methods := []string{}
	savedStates := []RuntimeScratchState{}
	taskBlocked := false
	sessionEnded := false
	runStatus := ""
	stepTitle := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "agent.session.blocked":
			t.Fatalf("routine dependency block must not create session blocker queue: %+v", req.Params)
		case "agent.task.block":
			taskBlocked = true
			writeRPCResult(w, req, nil)
		case "workspace.execution.run.write":
			runStatus = rpcString(req.Params, "status")
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.execution.step.write":
			stepTitle = rpcString(req.Params, "title")
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.tension.refresh":
			writeRPCResult(w, req, map[string]any{"workspace_id": "ws-1", "refresh": map[string]any{}})
		case "agent.session.end":
			sessionEnded = true
			if rpcString(req.Params, "status") != "ENDED" {
				t.Fatalf("expected dependency block to end session, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:   rpcString(req.Params, "session_id"),
					WorkspaceID: rpcString(req.Params, "workspace_id"),
					AgentID:     rpcString(req.Params, "agent_id"),
					TaskID:      rpcString(req.Params, "task_id"),
					Status:      "ENDED",
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-05T00:00:01Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-05-05T00:00:00Z",
					"updated_at":       "2026-05-05T00:00:01Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace":        map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during dependency block: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      "task-1",
		Status:      "ACTIVE",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.blockTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Waiting on gamma to publish review-ready branch",
		BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "gamma implementation task must finish first"}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("blockTaskCycle() error: %v", err)
	}
	if !taskBlocked || !sessionEnded || runStatus != "BLOCKED" || stepTitle != "Record dependency block" {
		t.Fatalf("expected task block + ended session evidence, taskBlocked=%v sessionEnded=%v runStatus=%q stepTitle=%q methods=%v", taskBlocked, sessionEnded, runStatus, stepTitle, methods)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected cleared scratch after dependency block, got no saved states")
	}
	lastSavedState := savedStates[len(savedStates)-1]
	if lastSavedState.ActiveSessionID != "" || lastSavedState.ActiveTaskID != "" || lastSavedState.ActiveRunID != "" {
		t.Fatalf("expected cleared scratch after dependency block, got %+v", savedStates)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state to be cleared, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestTransientProviderTimeoutYieldsAndReleasesWithoutOperatorQueue(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-provider-timeout",
		Title:        "Root coordination",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-provider-timeout",
		AgentID:   self,
		TaskID:    task.TaskID,
		Status:    "ACTIVE",
	}
	methods := []string{}
	taskReleased := false
	sessionEnded := false
	runStatus := ""
	runOutcome := ""
	stepStatus := ""
	stepTitle := ""
	var savedStates []RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.session.blocked":
			t.Fatalf("transient provider timeout must not open an operator blocker: %+v", req.Params)
		case "agent.task.block":
			t.Fatalf("transient provider timeout must release for retry instead of blocking task: %+v", req.Params)
		case "workspace.execution.run.write":
			runStatus = rpcString(req.Params, "status")
			runOutcome = rpcString(req.Params, "outcome")
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.execution.step.write":
			stepStatus = rpcString(req.Params, "status")
			stepTitle = rpcString(req.Params, "title")
			verification, _ := req.Params["verification"].(map[string]any)
			if verification["operator_queue_required"] != false || verification["transient_provider_timeout"] != true {
				t.Fatalf("expected transient timeout verification without operator queue, got %+v", verification)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.session.end":
			sessionEnded = true
			if got := rpcString(req.Params, "status"); got != "ENDED" {
				t.Fatalf("expected ENDED session status, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:   rpcString(req.Params, "session_id"),
					WorkspaceID: rpcString(req.Params, "workspace_id"),
					AgentID:     rpcString(req.Params, "agent_id"),
					TaskID:      rpcString(req.Params, "task_id"),
					Status:      "ENDED",
					Summary:     rpcString(req.Params, "summary"),
				},
			})
		case "agent.task.release":
			taskReleased = true
			if got := rpcString(req.Params, "task_id"); got != task.TaskID {
				t.Fatalf("released wrong task: %+v", req.Params)
			}
			if !strings.Contains(strings.ToLower(rpcString(req.Params, "reason")), "transient llm provider timeout") {
				t.Fatalf("expected transient provider timeout release reason, got %+v", req.Params)
			}
			if got := rpcString(req.Params, "session_transition_kind"); got != "reclaim_release" {
				t.Fatalf("transient provider timeout release must tolerate ended-session cleanup via reclaim_release, got %+v", req.Params)
			}
			writeRPCResult(w, req, nil)
		case "agent.state.set":
			var saved RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + shortHash(rpcString(req.Params, "doc_key"))})
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-06-04T00:00:00Z",
				"agent": map[string]any{
					"agent_id":     self,
					"workspace_id": "ws-1",
				},
				"snapshot": map[string]any{},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected method during transient provider timeout yield: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-provider-timeout",
		scratch: RuntimeScratchState{
			ActiveTaskID:    task.TaskID,
			ActiveSessionID: session.SessionID,
			ActiveRunID:     "run-provider-timeout",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.yieldTransientProviderTimeoutTaskCycle(context.Background(), task, session, "run-provider-timeout", fmt.Errorf("iteration 6: llm provider call timed out after 10m0s: %w", context.DeadlineExceeded), nil, nil)
	if err != nil {
		t.Fatalf("yieldTransientProviderTimeoutTaskCycle() error: %v", err)
	}
	if !sessionEnded || !taskReleased {
		t.Fatalf("expected session end and task release, ended=%v released=%v methods=%v", sessionEnded, taskReleased, methods)
	}
	if runStatus != "TIMED_OUT" || runOutcome != "TRANSIENT_PROVIDER_TIMEOUT" {
		t.Fatalf("expected TIMED_OUT run with transient outcome, status=%q outcome=%q", runStatus, runOutcome)
	}
	if stepStatus != "TIMED_OUT" || stepTitle != "Yield transient provider timeout" {
		t.Fatalf("expected timed-out yield step, status=%q title=%q", stepStatus, stepTitle)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected active scratch to be persisted clear, methods=%v", methods)
	}
	last := savedStates[len(savedStates)-1]
	if last.ActiveTaskID != "" || last.ActiveSessionID != "" || last.ActiveRunID != "" {
		t.Fatalf("expected active scratch clear, got %+v", last)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state clear, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestPinnedTransientProviderTimeoutRetainsClaimAndActiveSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-pinned-eval",
		Title:        "Pinned eval task",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-pinned-timeout",
		AgentID:   self,
		TaskID:    task.TaskID,
		Status:    "ACTIVE",
	}
	methods := []string{}
	sessionKeptAlive := false
	runStatus := ""
	runOutcome := ""
	stepTitle := ""
	var savedStates []RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "agent.task.release":
			t.Fatalf("pinned transient timeout must retain the task claim: %+v", req.Params)
		case "agent.session.end":
			t.Fatalf("pinned transient timeout must keep the session active: %+v", req.Params)
		case "workspace.execution.run.write":
			runStatus = rpcString(req.Params, "status")
			runOutcome = rpcString(req.Params, "outcome")
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.execution.step.write":
			stepTitle = rpcString(req.Params, "title")
			verification, _ := req.Params["verification"].(map[string]any)
			if verification["task_claim_transition"] != "retain_claim_for_retry" || verification["operator_authored_harness"] != true {
				t.Fatalf("expected pinned retain verification, got %+v", verification)
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.session.keepalive":
			sessionKeptAlive = true
			if got := rpcString(req.Params, "status"); got != "ACTIVE" {
				t.Fatalf("expected ACTIVE keepalive status, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:   rpcString(req.Params, "session_id"),
					WorkspaceID: rpcString(req.Params, "workspace_id"),
					AgentID:     rpcString(req.Params, "agent_id"),
					TaskID:      rpcString(req.Params, "task_id"),
					Status:      "ACTIVE",
					Summary:     rpcString(req.Params, "summary"),
				},
			})
		case "agent.state.set":
			var saved RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + shortHash(rpcString(req.Params, "doc_key"))})
		default:
			t.Fatalf("unexpected method during pinned transient provider timeout retain: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
			PinnedTaskID: task.TaskID,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-pinned-timeout",
		scratch: RuntimeScratchState{
			ActiveTaskID:    task.TaskID,
			ActiveSessionID: session.SessionID,
			ActiveRunID:     "run-pinned-timeout",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.yieldTransientProviderTimeoutTaskCycle(context.Background(), task, session, "run-pinned-timeout", fmt.Errorf("iteration 6: llm provider call timed out after 18m0s: %w", context.DeadlineExceeded), nil, nil)
	if err != nil {
		t.Fatalf("yieldTransientProviderTimeoutTaskCycle() error: %v", err)
	}
	if !sessionKeptAlive {
		t.Fatalf("expected pinned timeout to keep the session active, methods=%v", methods)
	}
	if runStatus != "TIMED_OUT" || runOutcome != "TRANSIENT_PROVIDER_TIMEOUT" {
		t.Fatalf("expected TIMED_OUT run with transient outcome, status=%q outcome=%q", runStatus, runOutcome)
	}
	if stepTitle != "Retain pinned task after transient provider timeout" {
		t.Fatalf("expected pinned retain step, got %q", stepTitle)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected scratch state writes, methods=%v", methods)
	}
	last := savedStates[len(savedStates)-1]
	if last.ActiveTaskID != task.TaskID || last.ActiveSessionID != session.SessionID || last.ActiveRunID != "run-pinned-timeout" {
		t.Fatalf("expected active scratch retained, got %+v", last)
	}
	if runtime.activeTask == nil || runtime.activeTask.TaskID != task.TaskID || runtime.activeSession == nil || runtime.activeSession.SessionID != session.SessionID || runtime.activeRunID != "run-pinned-timeout" {
		t.Fatalf("expected runtime active state retained, task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestIsTransientProviderTimeoutErrorRequiresProviderMarker(t *testing.T) {
	if !isTransientProviderTimeoutError(fmt.Errorf("iteration 1: llm provider call timed out after 10m0s: %w", context.DeadlineExceeded)) {
		t.Fatal("expected provider timeout marker to classify as transient provider timeout")
	}
	codexTimeout := fmt.Errorf("codex exec timed out: %w (output: )", context.DeadlineExceeded)
	if !isTransientProviderTimeoutError(fmt.Errorf("iteration 15: llm provider call timed out after 10m0s: %w", codexTimeout)) {
		t.Fatal("expected nested codex exec provider timeout to classify as transient provider timeout")
	}
	if isTransientProviderTimeoutError(fmt.Errorf("task cycle timed out: %w", context.DeadlineExceeded)) {
		t.Fatal("plain task-cycle deadline must not classify as provider timeout")
	}
	if isTransientProviderTimeoutError(context.Canceled) {
		t.Fatal("cancellation must not classify as provider timeout")
	}
}

func TestIsTransientProviderCapacityError(t *testing.T) {
	err := fmt.Errorf("iteration 1: codex exec failed: exit status 1 (output: ERROR: Selected model is at capacity. Please try a different model.)")
	if !isTransientProviderCapacityError(err) {
		t.Fatal("expected provider capacity error to classify as transient")
	}
	if !isTransientProviderCapacityError(fmt.Errorf("provider response: HTTP 429 rate limit exceeded")) {
		t.Fatal("expected provider rate limit to classify as transient")
	}
	if isTransientProviderCapacityError(fmt.Errorf("schema validation failed: missing required field path")) {
		t.Fatal("non-provider contract errors must not classify as provider capacity")
	}
	if isTransientProviderCapacityError(context.Canceled) {
		t.Fatal("plain cancellation must not classify as provider capacity")
	}
}

// TestBuildTimeoutResumeSummaryIsBoundedAndCarriesContext checks that the
// persisted resume summary stays under the 2KB ceiling and carries
// task id, iteration count, last tools, and the timeout cause.
func TestBuildTimeoutResumeSummaryIsBoundedAndCarriesContext(t *testing.T) {
	task := WorkspaceTaskRecord{TaskID: "task-timeout-resume", Title: "Implement feature"}
	trace := &TaskRunTrace{
		AssistantTurns: 7,
		ToolCalls:      []string{"read_file", "list_directory", "shell"},
		ToolReceipts: []TaskRunToolReceipt{
			{ToolName: "read_file", Output: "package main\nfunc main(){}"},
			{ToolName: "shell", IsError: true, Output: "exit status 1: build failed"},
		},
	}
	execErr := fmt.Errorf("iteration 6: llm provider call timed out after 10m0s: %w", context.DeadlineExceeded)

	summary := buildTimeoutResumeSummary(task, trace, execErr)
	if len(summary) > timeoutResumeSummaryMaxBytes {
		t.Fatalf("summary exceeds 2KB bound: %d bytes", len(summary))
	}
	for _, want := range []string{"task-timeout-resume", "iterations_before_timeout: 7", "last_tools", "timeout_cause", "llm provider call timed out"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	// Oversized trace must still be clamped to the ceiling.
	big := &TaskRunTrace{AssistantTurns: 30}
	for i := 0; i < 50; i++ {
		big.ToolReceipts = append(big.ToolReceipts, TaskRunToolReceipt{ToolName: "read_file", Output: strings.Repeat("x", 500)})
	}
	if got := len(buildTimeoutResumeSummary(task, big, execErr)); got > timeoutResumeSummaryMaxBytes {
		t.Fatalf("oversized summary not clamped: %d bytes", got)
	}
}

// TestTimeoutResumeSummaryPersistsAndSurfacesNextCycle checks that persisting
// the summary survives into scratch keyed by the task, and the next
// cycle for the SAME task surfaces it in the assembled prompt (while an
// unrelated task does not).
func TestTimeoutResumeSummaryPersistsAndSurfacesNextCycle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	self := "agent-te41"
	var savedStates []RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var saved RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			savedStates = append(savedStates, saved)
			writeRPCResult(w, req, nil)
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{TaskID: "task-timeout-resume-next-cycle", Title: "Resume me"}
	session := AgentSessionStateRecord{SessionID: "session-te41", AgentID: self, TaskID: task.TaskID}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:        RuntimeModeDaemon,
			Workdir:     t.TempDir(),
			RhizomeRPC:  server.URL,
			WorkspaceID: "ws-te41",
			AgentID:     self,
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	trace := &TaskRunTrace{AssistantTurns: 4, ToolReceipts: []TaskRunToolReceipt{{ToolName: "read_file", Output: "found the bug site"}}}
	execErr := fmt.Errorf("iteration 4: llm provider call timed out after 10m0s: %w", context.DeadlineExceeded)

	if err := runtime.persistTimeoutResumeSummary(context.Background(), task, session, "run-te41", trace, execErr); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if len(savedStates) == 0 {
		t.Fatal("expected scratch persistence call")
	}
	last := savedStates[len(savedStates)-1]
	if last.TimeoutResumeTaskID != task.TaskID || strings.TrimSpace(last.TimeoutResumeSummary) == "" {
		t.Fatalf("expected persisted resume summary keyed by task, got %+v", last)
	}
	if last.TimeoutResumeCount != 1 {
		t.Fatalf("expected resume count 1, got %d", last.TimeoutResumeCount)
	}

	// The same task surfaces the summary; an unrelated task does not.
	if got := runtime.timeoutResumeAdvisoryForTask(task); got == "" {
		t.Fatal("expected resume advisory for the timed-out task")
	}
	if got := runtime.timeoutResumeAdvisoryForTask(WorkspaceTaskRecord{TaskID: "other-task"}); got != "" {
		t.Fatalf("resume advisory must be task-keyed, leaked to other task: %q", got)
	}

	// The summary, injected as an advisory signal, must render into the prompt.
	advisory := runtime.timeoutResumeAdvisoryForTask(task)
	agent := &Agent{Workdir: t.TempDir()}
	prompt := agent.buildSystemPrompt(AgentTaskContext{Mode: "daemon", AdvisorySignals: []string{advisory}})
	if !strings.Contains(prompt, "TIMEOUT RESUME SUMMARY") {
		t.Fatalf("assembled prompt missing timeout resume summary:\n%s", prompt)
	}

	// Persisting again for the same task bumps the count (cross-cycle persistence).
	if err := runtime.persistTimeoutResumeSummary(context.Background(), task, session, "run-te41-b", trace, execErr); err != nil {
		t.Fatalf("persist second: %v", err)
	}
	if got := runtime.scratch.TimeoutResumeCount; got != 2 {
		t.Fatalf("expected resume count 2 after second timeout, got %d", got)
	}

	// Once cleared (resume consumed), it no longer surfaces.
	if err := runtime.clearTimeoutResumeSummary(context.Background(), task.TaskID); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := runtime.timeoutResumeAdvisoryForTask(task); got != "" {
		t.Fatalf("resume advisory must be cleared after consumption, got %q", got)
	}
}

func TestBlockTaskCycleAppliesBlockBeforeTerminalMaterialization(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	methods := []string{}
	taskBlockIndex := -1
	firstDocPutIndex := -1
	materializationDegraded := false
	runOutcomes := []string{}
	savedStates := []RuntimeScratchState{}

	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      "task-1",
		Status:      "ACTIVE",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: task.TaskID, Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "agent.session.blocked":
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:   session.SessionID,
					WorkspaceID: "ws-1",
					AgentID:     self,
					TaskID:      task.TaskID,
					Status:      "BLOCKED",
					Summary:     rpcString(req.Params, "summary"),
				},
			})
		case "agent.task.block":
			taskBlockIndex = len(methods) - 1
			writeRPCResult(w, req, map[string]any{"task_id": task.TaskID, "status": "BLOCKED"})
		case "workspace.doc.put":
			if firstDocPutIndex == -1 {
				firstDocPutIndex = len(methods) - 1
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key":    rpcString(req.Params, "doc_key"),
				"sha":        "sha-existing",
				"content":    "",
				"updated_at": "2026-05-18T00:00:00Z",
			})
		case "workspace.artifact.write":
			writeRPCResult(w, req, map[string]any{
				"artifact": WorkspaceArtifactRecord{
					ArtifactID:  "artifact-1",
					ArtifactRef: rpcString(req.Params, "artifact_ref"),
				},
			})
		case "agent.update.post":
			writeRPCError(w, req, -32000, "post update failed after docs")
		case "workspace.execution.step.write":
			if rpcString(req.Params, "phase") == "MATERIALIZE" && rpcString(req.Params, "status") == "DEGRADED" {
				materializationDegraded = true
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.tension.refresh":
			writeRPCResult(w, req, map[string]any{"workspace_id": "ws-1", "refresh": map[string]any{}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err == nil {
				savedStates = append(savedStates, state)
			}
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-18T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-05-18T00:00:00Z",
					"updated_at":       "2026-05-18T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace":        map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during blocked terminal materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.blockTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Blocked after producing final notes",
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: "local runtime missing follow-up input"}},
		Materialize: TaskMaterialization{
			DocKey:     "task.final",
			DocTitle:   "Task Final",
			DocContent: "final notes",
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("blockTaskCycle() error: %v", err)
	}
	if taskBlockIndex == -1 || firstDocPutIndex == -1 || taskBlockIndex > firstDocPutIndex {
		t.Fatalf("expected agent.task.block before terminal materialization doc writes, block=%d doc=%d methods=%v", taskBlockIndex, firstDocPutIndex, methods)
	}
	if !materializationDegraded {
		t.Fatalf("expected degraded materialization step after update failure, methods=%v", methods)
	}
	if len(runOutcomes) == 0 || runOutcomes[len(runOutcomes)-1] != "BLOCKED" {
		t.Fatalf("expected blocked run despite materialization degradation, got outcomes=%v methods=%v", runOutcomes, methods)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected cleared scratch after blocked materialization, got no saved states")
	}
	lastSavedState := savedStates[len(savedStates)-1]
	if lastSavedState.ActiveTaskID != "" || lastSavedState.ActiveSessionID != "" || lastSavedState.ActiveRunID != "" {
		t.Fatalf("expected cleared scratch after blocked materialization, got %+v", savedStates)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state to be cleared, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestBlockTaskCycleRefreshesTensionProjectionInDaemonMode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	methods := []string{}
	var refreshParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "agent.session.blocked":
			writeRPCResult(w, req, map[string]any{
				"state": AgentSessionStateRecord{
					SessionID:         rpcString(req.Params, "session_id"),
					WorkspaceID:       rpcString(req.Params, "workspace_id"),
					AgentID:           rpcString(req.Params, "agent_id"),
					TaskID:            rpcString(req.Params, "task_id"),
					Status:            rpcString(req.Params, "status"),
					Summary:           rpcString(req.Params, "summary"),
					OwnerScope:        rpcString(req.Params, "owner_scope"),
					KeepSessionActive: boolPtr(false),
					UpdatedAt:         "2026-04-28T02:00:00Z",
					StartedAt:         "2026-04-28T02:00:00Z",
				},
			})
		case "agent.task.block":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.tension.refresh":
			refreshParams = req.Params
			writeRPCResult(w, req, map[string]any{"workspace_id": rpcString(req.Params, "workspace_id"), "refresh": map[string]any{}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-28T02:00:01Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-28T02:00:00Z",
					"updated_at":       "2026-04-28T02:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"docs": []any{},
					"agents": []AgentRecord{
						{AgentID: self, WorkspaceID: "ws-1", OwnerUserID: "owner-1", DisplayName: "Agent One", Role: "generalist", Status: "ACTIVE", ProtocolVersion: "rnar/v1", Capabilities: []string{"tool.call"}, Summary: "bootstrapped", CreatedAt: "2026-04-28T02:00:00Z", UpdatedAt: "2026-04-28T02:00:00Z", IsOnline: true},
					},
					"sessions": []AgentSessionStateRecord{
						{SessionID: "session-1", WorkspaceID: "ws-1", AgentID: self, TaskID: "task-1", Status: "BLOCKED", Summary: "Need peer evidence", UpdatedAt: "2026-04-28T02:00:00Z", StartedAt: "2026-04-28T02:00:00Z"},
					},
					"tools": []any{},
					"tasks": []WorkspaceTaskRecord{
						{TaskID: "task-1", Title: "Task One", Status: "BLOCKED", ClaimAgentID: &self, ClaimStatus: &claimed},
					},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected RPC method during block tension refresh test: %s", req.Method)
		}
	}))
	defer server.Close()

	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      "task-1",
		Status:      "ACTIVE",
		Summary:     "working",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      self,
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-1",
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-1",
			ActiveSessionID: "session-1",
			ActiveRunID:     "run-1",
			DocSHAs:         map[string]string{},
		},
	}

	err := runtime.blockTaskCycle(context.Background(), task, session, "run-1", StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "Need peer evidence",
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: "missing reviewer response"}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("blockTaskCycle() error: %v", err)
	}
	if refreshParams == nil {
		t.Fatalf("expected workspace.tension.refresh after blocked transition, methods=%v", methods)
	}
	if got := rpcString(refreshParams, "workspace_id"); got != "ws-1" {
		t.Fatalf("refresh workspace_id = %q, want ws-1", got)
	}
	if got := rpcString(refreshParams, "actor_id"); got != self {
		t.Fatalf("refresh actor_id = %q, want %s", got, self)
	}
	if got, _ := refreshParams["limit"].(float64); got != 100 {
		t.Fatalf("refresh limit = %v, want 100", refreshParams["limit"])
	}
	if got, _ := refreshParams["cluster_limit"].(float64); got != 20 {
		t.Fatalf("refresh cluster_limit = %v, want 20", refreshParams["cluster_limit"])
	}
	stepIdx, refreshIdx, stateAfterRefreshIdx := -1, -1, -1
	for i, method := range methods {
		if method == "workspace.execution.step.write" && stepIdx == -1 {
			stepIdx = i
		}
		if method == "workspace.tension.refresh" && refreshIdx == -1 {
			refreshIdx = i
		}
		if method == "agent.state.set" && refreshIdx != -1 && i > refreshIdx && stateAfterRefreshIdx == -1 {
			stateAfterRefreshIdx = i
		}
	}
	if stepIdx == -1 || refreshIdx == -1 || stateAfterRefreshIdx == -1 || !(stepIdx < refreshIdx && refreshIdx < stateAfterRefreshIdx) {
		t.Fatalf("expected tension refresh after durable step and before terminal scratch write, got methods=%v", methods)
	}
}

func TestPlannerCadenceRefreshesTensionProjectionWithThrottle(t *testing.T) {
	refreshes := 0
	var refreshParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tension.refresh":
			refreshes++
			refreshParams = req.Params
			writeRPCResult(w, req, map[string]any{"workspace_id": rpcString(req.Params, "workspace_id"), "refresh": map[string]any{}})
		default:
			t.Fatalf("unexpected RPC method during planner cadence tension refresh test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	runtime.maybeRefreshTensionProjectionOnPlannerCadence(context.Background())
	runtime.maybeRefreshTensionProjectionOnPlannerCadence(context.Background())
	if refreshes != 1 {
		t.Fatalf("expected one refresh inside throttle window, got %d", refreshes)
	}
	if got := rpcString(refreshParams, "workspace_id"); got != "ws-1" {
		t.Fatalf("refresh workspace_id = %q, want ws-1", got)
	}
	if got := rpcString(refreshParams, "actor_id"); got != "agent-1" {
		t.Fatalf("refresh actor_id = %q, want agent-1", got)
	}

	runtime.mu.Lock()
	runtime.lastTensionProjectionRefresh = time.Now().UTC().Add(-runtimeTensionProjectionRefreshCadence - time.Second)
	runtime.mu.Unlock()
	runtime.maybeRefreshTensionProjectionOnPlannerCadence(context.Background())
	if refreshes != 2 {
		t.Fatalf("expected refresh after throttle window, got %d", refreshes)
	}
}

func TestExecuteTaskCycleFencesLateCompletionBeforeMaterialization(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	other := "agent-other"
	claimed := "CLAIMED"
	methods := []string{}
	runOutcomes := []string{}
	stepPhases := []string{}
	stepStatuses := []string{}
	savedStates := []RuntimeScratchState{}
	claimedWorkDocWrites := 0
	currentContextDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			stepPhases = append(stepPhases, rpcString(req.Params, "phase"))
			stepStatuses = append(stepStatuses, rpcString(req.Params, "status"))
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &other, ClaimStatus: &claimed},
				},
			})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{
				"run": map[string]any{
					"run_id":  rpcString(req.Params, "run_id"),
					"status":  rpcString(req.Params, "status"),
					"outcome": rpcString(req.Params, "outcome"),
				},
			})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			switch docKey {
			case agentContextDocKey(self):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected cleared current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-context-cleared"})
			case claimedWorkDocKey(self):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected cleared claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-cleared"})
			default:
				t.Fatalf("late fenced completion should not materialize doc %q: %+v", docKey, req.Params)
			}
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-18T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-18T00:00:00Z",
					"updated_at":       "2026-04-18T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.task.complete", "agent.session.end", "agent.update.post", "workspace.artifact.write":
			t.Fatalf("late fenced completion reached forbidden canonical mutation %s", req.Method)
		default:
			t.Fatalf("unexpected method during late completion fencing: %s", req.Method)
		}
	}))
	defer server.Close()

	taskClaimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &taskClaimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-1",
		TaskID:    "task-1",
		Status:    "ACTIVE",
	}
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
	}
	cfg.ApplyDefaults()
	llm := &sequenceLLM{responses: []*LLMResponse{{
		Content: `{"outcome":"completed","summary":"Finished after stale takeover","materialize":{"doc_key":"task.final","doc_title":"Final","doc_content":"must not write"}}`,
	}}}
	runtime := NewRuntime(cfg, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    "task-1",
		ActiveSessionID: "session-1",
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error: %v", err)
	}

	if strings.Join(methods, ",") == "" {
		t.Fatal("expected RPC calls")
	}
	for _, forbidden := range []string{"agent.task.complete", "agent.session.end", "agent.update.post", "workspace.artifact.write"} {
		for _, method := range methods {
			if method == forbidden {
				t.Fatalf("forbidden method %s was called in late completion path: %v", forbidden, methods)
			}
		}
	}
	if !containsString(runOutcomes, "FENCED_LATE") {
		t.Fatalf("expected FENCED_LATE execution run outcome, got outcomes=%v methods=%v", runOutcomes, methods)
	}
	if len(stepPhases) < 3 || stepPhases[len(stepPhases)-1] != "VERIFY" || stepStatuses[len(stepStatuses)-1] != "BLOCKED" {
		t.Fatalf("expected final blocked VERIFY step for fenced late completion, phases=%v statuses=%v", stepPhases, stepStatuses)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected one current-context and one claimed-work cleanup doc write, got current=%d claimed=%d", currentContextDocWrites, claimedWorkDocWrites)
	}
	if len(savedStates) == 0 || savedStates[0].ActiveTaskID != "" || savedStates[0].ActiveSessionID != "" || savedStates[0].ActiveRunID != "" {
		t.Fatalf("expected active state cleared after late completion fence, got %+v", savedStates)
	}
}

func TestExecuteTaskCycleRechecksCompletionAuthorityBeforeMaterializationRace(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	other := "agent-other"
	claimed := "CLAIMED"
	methods := []string{}
	runOutcomes := []string{}
	taskListCalls := 0
	claimedWorkDocWrites := 0
	currentContextDocWrites := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			taskListCalls++
			owner := self
			if taskListCalls > 1 {
				owner = other
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &owner, ClaimStatus: &claimed},
				},
			})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			docKey := rpcString(req.Params, "doc_key")
			switch docKey {
			case agentContextDocKey(self):
				currentContextDocWrites++
				content := rpcString(req.Params, "content")
				if !strings.Contains(content, "- outcome: idle") || !strings.Contains(content, "- task_id: (none)") {
					t.Fatalf("expected cleared current context doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-context-cleared"})
			case claimedWorkDocKey(self):
				claimedWorkDocWrites++
				if !strings.Contains(rpcString(req.Params, "content"), "active_claimed_work: none") {
					t.Fatalf("expected cleared claimed work doc, got %+v", req.Params)
				}
				writeRPCResult(w, req, map[string]any{"sha": "sha-claimed-cleared"})
			default:
				t.Fatalf("completion race should not materialize doc %q before second authority check: %+v", docKey, req.Params)
			}
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-18T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-18T00:00:00Z",
					"updated_at":       "2026-04-18T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.task.complete", "agent.session.end", "agent.update.post", "workspace.artifact.write":
			t.Fatalf("completion race reached forbidden canonical mutation %s", req.Method)
		default:
			t.Fatalf("unexpected method during completion race fencing: %s", req.Method)
		}
	}))
	defer server.Close()

	taskClaimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &taskClaimed,
	}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
	}
	cfg.ApplyDefaults()
	llm := &sequenceLLM{responses: []*LLMResponse{{
		Content: `{"outcome":"completed","summary":"Finished during ownership race","materialize":{"doc_key":"task.final","doc_title":"Final","doc_content":"must not write"}}`,
	}}}
	runtime := NewRuntime(cfg, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    "task-1",
		ActiveSessionID: "session-1",
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error: %v", err)
	}
	if taskListCalls < 2 {
		t.Fatalf("expected pre-materialization and terminal authority checks, got %d calls via %v", taskListCalls, methods)
	}
	if !containsString(runOutcomes, "FENCED_LATE") {
		t.Fatalf("expected FENCED_LATE outcome after second authority check, got outcomes=%v methods=%v", runOutcomes, methods)
	}
	if currentContextDocWrites != 1 || claimedWorkDocWrites != 1 {
		t.Fatalf("expected one current-context and one claimed-work cleanup doc write, got current=%d claimed=%d via %v", currentContextDocWrites, claimedWorkDocWrites, methods)
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func TestIsDocumentConflictErrorMatchesRPCCode(t *testing.T) {
	if !isDocumentConflictError(&RhizomeRPCError{Method: "workspace.doc.put", Code: rhizomeRPCCodeDocumentConflict, Message: "sha drifted"}) {
		t.Fatal("expected document conflict detector to match rpc code despite message drift")
	}
	if isDocumentConflictError(&RhizomeRPCError{Method: "workspace.doc.put", Code: -32000, Message: "sha drifted"}) {
		t.Fatal("did not expect document conflict detector to match unrelated rpc code without legacy text")
	}
	if !isDocumentConflictError(errors.New("rpc workspace.doc.put: document conflict")) {
		t.Fatal("expected document conflict detector to keep legacy text fallback")
	}
}

func TestPutDocWithRetryUsesDocumentConflictRetryFloor(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	methods := []string{}
	putCalls := 0
	getCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.doc.put":
			putCalls++
			writeRPCError(w, req, -32000, "document conflict")
		case "workspace.doc.get":
			getCalls++
			writeRPCResult(w, req, map[string]any{
				"doc_key":    req.Params["doc_key"],
				"sha":        "sha-remote",
				"content":    "remote content",
				"updated_at": "2026-04-21T06:00:00Z",
			})
		default:
			t.Fatalf("unexpected RPC method for retry ceiling test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:                  t.TempDir(),
			RhizomeRPC:               server.URL,
			RhizomeToken:             "token",
			WorkspaceID:              "ws-1",
			AgentID:                  "agent-1",
			MaxProviderRetryAttempts: 2,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{"agent.agent-1.current_context": "sha-local"},
		},
	}

	err := runtime.putDocWithRetry(context.Background(), "agent.agent-1.current_context", "Agent Current Context", "local content")
	if err == nil {
		t.Fatal("expected provider retry ceiling error")
	}
	if !strings.Contains(err.Error(), "retry ceiling") {
		t.Fatalf("expected ceiling error, got %v", err)
	}
	if putCalls != defaultDocConflictRetryLimit || getCalls != defaultDocConflictRetryLimit-1 {
		t.Fatalf("expected document conflict retry floor before ceiling, got puts=%d gets=%d methods=%v", putCalls, getCalls, methods)
	}
	if runtime.scratch.DocSHAs["agent.agent-1.current_context"] != "sha-local" {
		t.Fatalf("expected scratch sha to remain unchanged after ceiling exhaustion, got %+v", runtime.scratch.DocSHAs)
	}
}

func TestFailTaskCycleBlocksEvenWhenMaterializationHitsProviderCeiling(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	methods := []string{}
	runStatuses := []string{}
	runOutcomes := []string{}
	taskStatuses := []string{}
	sessionStatuses := []string{}
	putCalls := 0
	getCalls := 0
	claimedAgent := "agent-1"
	claimedStatus := "CLAIMED"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: "task-1", Status: "RUNNING", ClaimAgentID: &claimedAgent, ClaimStatus: &claimedStatus},
				},
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "- outcome: idle") || strings.Contains(content, "active_claimed_work: none") {
				writeRPCResult(w, req, map[string]any{"sha": "sha-cleanup"})
				return
			}
			putCalls++
			writeRPCError(w, req, -32000, "document conflict")
		case "workspace.doc.get":
			getCalls++
			writeRPCResult(w, req, map[string]any{
				"doc_key":    rpcString(req.Params, "doc_key"),
				"sha":        "sha-remote",
				"content":    "remote content",
				"updated_at": "2026-04-21T06:00:00Z",
			})
		case "agent.session.blocked":
			sessionStatuses = append(sessionStatuses, rpcString(req.Params, "status"))
			writeRPCResult(w, req, map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        rpcString(req.Params, "workspace_id"),
				"agent_id":            rpcString(req.Params, "agent_id"),
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              rpcString(req.Params, "status"),
				"summary":             rpcString(req.Params, "summary"),
				"owner_scope":         rpcString(req.Params, "owner_scope"),
				"blocked_on":          []any{},
				"keep_session_active": false,
				"updated_at":          "2026-04-21T06:00:00Z",
				"started_at":          "2026-04-21T06:00:00Z",
			})
		case "agent.task.block":
			taskStatuses = append(taskStatuses, "BLOCKED")
			writeRPCResult(w, req, nil)
		case "workspace.execution.run.write":
			runStatuses = append(runStatuses, rpcString(req.Params, "status"))
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, nil)
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-18T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-18T00:00:00Z",
					"updated_at":       "2026-04-18T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws-1",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected RPC method during failure fallback test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:                  t.TempDir(),
			RhizomeRPC:               server.URL,
			RhizomeToken:             "token",
			WorkspaceID:              "ws-1",
			AgentID:                  "agent-1",
			MaxProviderRetryAttempts: 1,
			MaxToolLoopIterations:    1,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}

	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &claimedAgent,
		ClaimStatus:  &claimedStatus,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-1",
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		TaskID:      "task-1",
		Status:      "ACTIVE",
		Summary:     "working",
	}

	err := runtime.failTaskCycle(context.Background(), task, session, "run-1", errors.New("tool loop exceeded 1 iterations"), nil, nil)
	if err != nil {
		t.Fatalf("failTaskCycle() error: %v", err)
	}

	if putCalls != defaultDocConflictRetryLimit || getCalls != defaultDocConflictRetryLimit-1 {
		t.Fatalf("expected materialization to exhaust document conflict retries before hard stop, got puts=%d gets=%d via %v", putCalls, getCalls, methods)
	}
	if len(runStatuses) == 0 || len(runOutcomes) == 0 || runStatuses[0] != "BLOCKED" || runOutcomes[0] != "BLOCKED" {
		t.Fatalf("expected blocked execution run after materialization failure, got statuses=%v outcomes=%v methods=%v", runStatuses, runOutcomes, methods)
	}
	if len(taskStatuses) != 1 || taskStatuses[0] != "BLOCKED" {
		t.Fatalf("expected task block terminalization, got task statuses=%v methods=%v", taskStatuses, methods)
	}
	if len(sessionStatuses) != 1 || sessionStatuses[0] != "BLOCKED" {
		t.Fatalf("expected blocked session terminalization, got session statuses=%v methods=%v", sessionStatuses, methods)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("expected runtime active state to be cleared after terminal block, got task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

func TestExecuteTaskCycleTerminalizesProviderDeadlineWithCleanupContext(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	methods := []string{}
	savedStates := []RuntimeScratchState{}
	sessionBlocked := false
	taskBlocked := false
	blockedRunWritten := false
	blockedStepWritten := false

	task := WorkspaceTaskRecord{
		TaskID:       "task-timeout",
		Title:        "Timeout task",
		Description:  "Exercise provider deadline terminalization",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID:   "session-timeout",
		WorkspaceID: "ws-1",
		AgentID:     self,
		TaskID:      task.TaskID,
		Status:      "ACTIVE",
		Summary:     "working",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			if rpcString(req.Params, "phase") == "VERIFY" && rpcString(req.Params, "status") == "BLOCKED" {
				blockedStepWritten = true
				if r.Context().Err() != nil {
					t.Fatalf("blocked VERIFY step inherited canceled request context: %v", r.Context().Err())
				}
			}
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{task},
			})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, nil)
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, nil)
		case "agent.session.blocked":
			sessionBlocked = true
			if r.Context().Err() != nil {
				t.Fatalf("session block inherited canceled request context: %v", r.Context().Err())
			}
			writeRPCResult(w, req, map[string]any{"state": AgentSessionStateRecord{
				SessionID:   session.SessionID,
				WorkspaceID: "ws-1",
				AgentID:     self,
				TaskID:      task.TaskID,
				Status:      "BLOCKED",
				Summary:     rpcString(req.Params, "summary"),
			}})
		case "agent.task.block":
			taskBlocked = true
			if r.Context().Err() != nil {
				t.Fatalf("task block inherited canceled request context: %v", r.Context().Err())
			}
			writeRPCResult(w, req, map[string]any{"task_id": task.TaskID, "status": "BLOCKED"})
		case "workspace.execution.run.write":
			if rpcString(req.Params, "status") == "BLOCKED" && rpcString(req.Params, "outcome") == "BLOCKED" {
				blockedRunWritten = true
				if r.Context().Err() != nil {
					t.Fatalf("blocked run inherited canceled request context: %v", r.Context().Err())
				}
			}
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "workspace.tension.refresh":
			writeRPCResult(w, req, map[string]any{"workspace_id": "ws-1", "refresh": map[string]any{}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err == nil {
				savedStates = append(savedStates, state)
			}
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-17T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-05-17T00:00:00Z",
					"updated_at":       "2026-05-17T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method during provider deadline terminalization: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &deadlineBlockingLLM{started: make(chan struct{})}
	cfg := RuntimeConfig{
		Mode:                     RuntimeModeDaemon,
		Workdir:                  t.TempDir(),
		RhizomeRPC:               server.URL,
		RhizomeToken:             "token",
		WorkspaceID:              "ws-1",
		AgentID:                  self,
		OwnerUserID:              "owner-1",
		PlannerCycleTimeout:      time.Second,
		MaxToolLoopIterations:    1,
		MaxProviderRetryAttempts: 1,
	}
	runtime := NewRuntime(cfg, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-timeout"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    task.TaskID,
		ActiveSessionID: session.SessionID,
		ActiveRunID:     "run-timeout",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	err := runtime.executeTaskCycle(ctx, task)
	if !errors.Is(err, errRuntimePlannerWorkCycleTimeout) {
		t.Fatalf("executeTaskCycle() error = %v, want %v", err, errRuntimePlannerWorkCycleTimeout)
	}
	select {
	case <-llm.started:
	default:
		t.Fatalf("provider deadline test never reached the LLM call; methods=%v err=%v", methods, err)
	}
	if !sessionBlocked || !taskBlocked || !blockedRunWritten || !blockedStepWritten {
		t.Fatalf("timeout did not terminalize the task cycle, sessionBlocked=%v taskBlocked=%v run=%v step=%v methods=%v", sessionBlocked, taskBlocked, blockedRunWritten, blockedStepWritten, methods)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected scratch state writes during timeout cleanup, methods=%v", methods)
	}
	last := savedStates[len(savedStates)-1]
	if last.ActiveTaskID != "" || last.ActiveSessionID != "" || last.ActiveRunID != "" {
		t.Fatalf("timeout cleanup left active scratch ids: %+v", last)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.activeRunID != "" {
		t.Fatalf("timeout cleanup left runtime active state, task=%+v session=%+v run=%q", runtime.activeTask, runtime.activeSession, runtime.activeRunID)
	}
}

type deadlineBlockingLLM struct {
	started chan struct{}
}

func (l *deadlineBlockingLLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	if l != nil && l.started != nil {
		close(l.started)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
