package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeOwnerBoundActiveTaskYieldsNonOwnerClaim(t *testing.T) {
	var methods []string
	var releaseReason string
	var endedHandoff string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedOwnerBoundProjectCoordination("gamma"))
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-owner-bound"})
		case "agent.task.release":
			releaseReason = rpcString(req.Params, "reason")
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			endedHandoff = rpcString(req.Params, "handoff_to")
			writeRPCResult(w, req, map[string]any{
				"session_id": req.Params["session_id"],
				"agent_id":   "iota",
				"task_id":    "task-submit",
				"status":     "ENDED",
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-presence"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	claimAgent := "iota"
	claimStatus := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-submit",
		Title:        "Owner-only project_patch_queue_submit",
		Description:  "Owner-only submit.\n\n- branch_id: branch-gamma",
		ProjectID:    "project-alpha",
		ProjectLane:  "integration",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "owner-branch:branch-gamma", "required-agent:gamma"},
		ClaimAgentID: &claimAgent,
		ClaimStatus:  &claimStatus,
	}
	session := &AgentSessionStateRecord{
		SessionID: "sess-owner-bound",
		AgentID:   "iota",
		TaskID:    "task-submit",
		Status:    "RUNNING",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "iota",
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: session,
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-submit",
			ActiveSessionID: "sess-owner-bound",
			DocSHAs:         map[string]string{},
		},
	}

	yielded, err := runtime.maybeYieldOwnerBoundActiveTask(context.Background(), task, session)
	if err != nil {
		t.Fatalf("maybeYieldOwnerBoundActiveTask() error = %v", err)
	}
	if !yielded {
		t.Fatal("expected non-owner owner-bound active claim to yield")
	}
	if runtime.activeTask != nil || runtime.activeSession != nil || runtime.scratch.ActiveTaskID != "" {
		t.Fatalf("expected active state cleared, task=%+v session=%+v scratch=%+v", runtime.activeTask, runtime.activeSession, runtime.scratch)
	}
	if !strings.Contains(releaseReason, "branch_id=branch-gamma") || endedHandoff != "gamma" {
		t.Fatalf("expected release/session handoff to branch owner, reason=%q handoff=%q", releaseReason, endedHandoff)
	}
	if !containsAll(methods, []string{"project.coordination.get", "agent.update.post", "agent.task.release", "agent.session.end", "agent.state.set", "workspace.doc.put"}) {
		t.Fatalf("expected yield evidence/release/session/scratch/doc methods, got %#v", methods)
	}
}

func TestRuntimeOwnerBoundActiveTaskYieldsBeforePendingTrigger(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, delegatedOwnerBoundProjectCoordination("gamma"))
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-owner-bound"})
		case "agent.task.release":
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			writeRPCResult(w, req, map[string]any{"session_id": req.Params["session_id"], "status": "ENDED"})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-presence"})
		case "agent.work.next", "agent.task.claim", "agent.session.start":
			t.Fatalf("owner-bound active yield must run before pending trigger work selection, got %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	claimAgent := "iota"
	claimStatus := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-submit",
		Title:        "Owner-only project_patch_queue_submit",
		Description:  "Owner-only submit.\n\n- branch_id: branch-gamma",
		ProjectID:    "project-alpha",
		ProjectLane:  "integration",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		Tags:         []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "owner-branch:branch-gamma", "required-agent:gamma"},
		ClaimAgentID: &claimAgent,
		ClaimStatus:  &claimStatus,
	}
	session := &AgentSessionStateRecord{
		SessionID: "sess-owner-bound",
		AgentID:   "iota",
		TaskID:    "task-submit",
		Status:    "RUNNING",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "iota",
		},
		client:        NewRhizomeClient(server.URL, "token"),
		activeTask:    &task,
		activeSession: session,
		scratch: RuntimeScratchState{
			ActiveTaskID:       "task-submit",
			ActiveSessionID:    "sess-owner-bound",
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-other",
			DocSHAs:            map[string]string{},
		},
	}

	got, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if got != nil {
		t.Fatalf("expected no runnable task after owner-bound yield, got %+v", got)
	}
	if runtime.activeTask != nil || runtime.scratch.ActiveTaskID != "" {
		t.Fatalf("expected owner-bound active state cleared before trigger, task=%+v scratch=%+v", runtime.activeTask, runtime.scratch)
	}
	if !containsAll(methods, []string{"project.coordination.get", "agent.update.post", "agent.task.release"}) {
		t.Fatalf("expected owner-bound release path before trigger, got %#v", methods)
	}
}

func TestRuntimeOwnerBoundRequirementTreatsTagBranchOwnerConflictAsRepair(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-submit-conflict",
		Title:       "Owner-only project_patch_queue_submit",
		Description: "Owner-only submit.\n\n- branch_id: branch-gamma",
		ProjectID:   "project-alpha",
		Tags:        []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "owner-branch:branch-gamma", "required-agent:iota"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if !req.RepairNeeded || req.RequiredAgentID != "gamma" || !strings.Contains(req.Reason, "conflicts with branch owner") {
		t.Fatalf("expected branch registry owner to win with repair-needed conflict, got %+v", req)
	}
}

func TestRuntimeOwnerBoundPatchQueueSubmitRequiresConcreteBranch(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:    "task-submit-missing-branch",
		Title:     "Owner-only project_patch_queue_submit",
		ProjectID: "project-alpha",
		Tags:      []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "required-agent:iota"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-alpha"},
	})
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if !req.RepairNeeded || !strings.Contains(req.Reason, "concrete branch") {
		t.Fatalf("expected missing branch to require repair, got %+v", req)
	}
}

func TestRuntimeOwnerBoundDetectionIgnoresHistoricalHintsInDescription(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-idle-reflection-owner-bound-hints",
		Title:       "Artifact quality iteration: inspect evidence and concrete gaps",
		Description: "Copied context from an old task list:\n- task-old [PENDING]: Run the owner-only patch-queue submit for the gamma branch\n\nThe current task is an artifact review.",
		ProjectID:   "project-alpha",
		ProjectLane: "qa",
		Tags:        []string{"meta-reflection", "anti-idle", "qa", "metacognition-scope-artifact"},
	}
	_, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-alpha"},
	})
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if ok {
		t.Fatal("description-only historical owner-only hints must not classify the current task as owner-bound")
	}
}

func TestRuntimeOwnerBoundDetectionLeavesOwnerSubmitValidationClaimable(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-validate-owner-submit-candidate",
		Title:       "Validate owner-submit candidate without claiming owner-submit",
		Description: "Patch queue decision follow-up.\n\nbranch_id: branch-gamma\n\nIf evidence passes, branch owner beta should call project_patch_queue_submit.",
		ProjectID:   "project-alpha",
		ProjectLane: "validation",
		Tags:        []string{"project", "patch-queue", "validation", "blocked"},
	}
	_, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-alpha"},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-gamma",
			ProjectID:  "project-alpha",
			AgentID:    "beta",
			BranchName: "agent/gamma/owner-bound-submit",
			Status:     "READY_FOR_REVIEW",
		}},
	})
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if ok {
		t.Fatal("negated owner-submit validation task must not classify as owner-bound")
	}
}

func TestRuntimeOwnerBoundPatchQueueSubmitInfersUniqueOwnerBranch(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:    "task-submit-owner-only",
		Title:     "Owner-only project_patch_queue_submit for gamma",
		ProjectID: "project-alpha",
		Tags:      []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "required-agent:gamma"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if req.RepairNeeded || req.BranchID != "branch-gamma" || req.BranchName != "agent/gamma/owner-bound-submit" || req.RequiredAgentID != "gamma" {
		t.Fatalf("expected unique gamma branch inference, got %+v", req)
	}
}

func TestRuntimeOwnerBoundPatchQueueSubmitInfersProseBranchMention(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("beta")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-requeue-branch-gamma-owner-submit",
		Title:       "Beta owner requeue submit for validated branch",
		Description: "Create a precise owner lane for branch `branch-gamma` / `agent/gamma/owner-bound-submit` without changing branch contents.",
		ProjectID:   "project-alpha",
		Tags:        []string{"project", "patch-queue", "requeue", "coordination", "owner-only", "beta"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if req.RepairNeeded || req.BranchID != "branch-gamma" || req.BranchName != "agent/gamma/owner-bound-submit" || req.RequiredAgentID != "beta" {
		t.Fatalf("expected prose branch mention to resolve owner, got %+v", req)
	}
}

func TestRuntimeOwnerSubmitTagInfersOwnerBoundRequirement(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("beta")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-requeue-owner-submit-icon-sprite-forge-dc954850-20260513",
		Title:       "Owner requeue submit for same-head patch queue item on branch-gamma",
		Description: "Create a fresh live COORDINATION task for branch owner beta to perform the owner-only requeue submission.\n\nbranch_id: branch-gamma",
		ProjectID:   "project-alpha",
		ProjectLane: "coordination",
		Tags:        []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-submit task to be owner-bound")
	}
	if req.RepairNeeded || req.BranchID != "branch-gamma" || req.BranchName != "agent/gamma/owner-bound-submit" || req.RequiredAgentID != "beta" {
		t.Fatalf("expected owner-submit task to resolve branch owner, got %+v", req)
	}
}

func TestRuntimeOwnerSubmitTagPrefersExactBranchIDOverReusedBranchName(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("beta")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	coordination.Branches = append(coordination.Branches, ProjectBranchRecord{
		BranchID:    "branch-gamma-old",
		WorkspaceID: "ws",
		ProjectID:   "project-alpha",
		RepoID:      "repo-main",
		AgentID:     "beta",
		BranchName:  "agent/gamma/owner-bound-submit",
		BranchKind:  "feature",
		Status:      "MERGED",
	})
	task := WorkspaceTaskRecord{
		TaskID:      "task-requeue-owner-submit-branch-gamma",
		Title:       "Beta owner-side same-head requeue submit for branch-gamma",
		Description: "Create the owner-side patch queue requeue submission on branch `branch-gamma` / `agent/gamma/owner-bound-submit`. Review doc: `project.project-alpha.branch.branch-gamma.review`.",
		ProjectID:   "project-alpha",
		ProjectLane: "coordination",
		Tags:        []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-submit task to be owner-bound")
	}
	if req.RepairNeeded || req.BranchID != "branch-gamma" || req.BranchName != "agent/gamma/owner-bound-submit" || req.RequiredAgentID != "beta" {
		t.Fatalf("expected exact branch id to resolve despite reused branch name, got %+v", req)
	}
}

func TestRuntimeOwnerBoundBranchMentionDoesNotMatchBranchIDPrefix(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-alpha"},
		Branches: []ProjectBranchRecord{
			{
				BranchID:   "projbranch-123",
				ProjectID:  "project-alpha",
				RepoID:     "repo-main",
				AgentID:    "beta",
				BranchName: "agent/beta/short",
				Status:     "READY_FOR_REVIEW",
			},
			{
				BranchID:   "projbranch-123-5892",
				ProjectID:  "project-alpha",
				RepoID:     "repo-main",
				AgentID:    "beta",
				BranchName: "agent/beta/long",
				Status:     "READY_FOR_REVIEW",
			},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:    "task-owner-submit",
		Title:     "Owner requeue submit for projbranch-123-5892",
		ProjectID: "project-alpha",
		Tags:      []string{"owner-submit"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if req.RepairNeeded || req.BranchID != "projbranch-123-5892" || req.RequiredAgentID != "beta" {
		t.Fatalf("expected long branch id to resolve without prefix ambiguity, got %+v", req)
	}
}

func TestRuntimeOwnerBoundIdentityBranchNameWinsOverDescriptionBranchID(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-alpha"},
		Branches: []ProjectBranchRecord{
			{
				BranchID:   "projbranch-title-name",
				ProjectID:  "project-alpha",
				RepoID:     "repo-main",
				AgentID:    "beta",
				BranchName: "agent/beta/title-target",
				Status:     "READY_FOR_REVIEW",
			},
			{
				BranchID:   "projbranch-description-id",
				ProjectID:  "project-alpha",
				RepoID:     "repo-main",
				AgentID:    "gamma",
				BranchName: "agent/gamma/description-noise",
				Status:     "READY_FOR_REVIEW",
			},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-submit-title-name",
		Title:       "Owner requeue submit for agent/beta/title-target",
		Description: "Copied context also mentions unrelated branch `projbranch-description-id`.",
		ProjectID:   "project-alpha",
		Tags:        []string{"owner-submit"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if req.RepairNeeded || req.BranchID != "projbranch-title-name" || req.RequiredAgentID != "beta" {
		t.Fatalf("expected identity branch name to resolve before description branch id, got %+v", req)
	}
}

func TestRuntimeDelegatedOwnerBoundRejectsAcceptedTerminalSubmit(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("beta")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	coordination.Branches[0].Status = "MERGED"
	coordination.Branches[0].HeadSHA = strings.Repeat("b", 40)
	coordination.PatchQueueItems = []ProjectPatchQueueItemRecord{
		{
			QueueID:   "queue-main",
			ItemID:    "item-accepted",
			ProjectID: "project-alpha",
			RepoID:    "repo-main",
			BranchID:  "branch-gamma",
			HeadSHA:   strings.Repeat("b", 40),
			State:     "ACCEPTED",
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": coordination})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "beta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-stale-owner-submit",
		Title:       "Owner requeue submit for branch-gamma",
		Description: "Submit already accepted candidate `item-accepted`.",
		ProjectID:   "project-alpha",
		Tags:        []string{"owner-submit", "patch-queue"},
	}
	blocker := runtime.delegatedOwnerBoundTaskBlocker(context.Background(), task)
	if !strings.Contains(blocker, "already has an ACCEPTED same-head patch queue decision") {
		t.Fatalf("expected accepted terminal owner-submit blocker, got %q", blocker)
	}
}

func TestRuntimeOwnerBoundPatchQueueSubmitRejectsAmbiguousProseBranchMentions(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("beta")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	coordination.Branches = append(coordination.Branches, ProjectBranchRecord{
		BranchID:    "branch-beta-2",
		WorkspaceID: "ws",
		ProjectID:   "project-alpha",
		RepoID:      "repo-main",
		AgentID:     "beta",
		BranchName:  "agent/beta/second-owner-submit",
		BranchKind:  "feature",
		Status:      "READY_FOR_REVIEW",
	})
	task := WorkspaceTaskRecord{
		TaskID:      "task-requeue-ambiguous-owner-submit",
		Title:       "Owner-only project_patch_queue_submit for beta",
		Description: "This stale task mentions both branch `branch-gamma` and branch `branch-beta-2`; do not guess.",
		ProjectID:   "project-alpha",
		Tags:        []string{"owner-only", "patch-queue", "beta"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if !req.RepairNeeded || !strings.Contains(req.Reason, "multiple registered branches") {
		t.Fatalf("expected ambiguous prose branch mentions to require repair, got %+v", req)
	}
}

func TestRuntimeOwnerBoundRecognizesPublicationGapSidecar(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-clearpress-editor-publication-gap",
		Title:       "Publish or release editor lane provenance for Clearpress MVP",
		Description: "Publish durable provenance for the current active implementation lane without claiming a duplicate sidecar.",
		ProjectID:   "project-alpha",
		ProjectLane: "coordination",
		Tags:        []string{"clearpress", "editor", "coordination", "publication-gap"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected publication-gap sidecar to be owner-bound")
	}
	if req.Kind != "active_lane_publication" || req.BranchID != "branch-gamma" || req.RequiredAgentID != "gamma" || req.RepairNeeded {
		t.Fatalf("expected active-lane publication to resolve branch owner, got %+v", req)
	}
}

func TestRuntimeOwnerBoundDoesNotClassifyIntegrationConvergenceAsPublicationSidecar(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	coordination.Branches = append(coordination.Branches, ProjectBranchRecord{
		BranchID:    "branch-eta",
		WorkspaceID: "ws",
		ProjectID:   "project-alpha",
		RepoID:      "repo-main",
		AgentID:     "eta",
		BranchName:  "agent/eta/evaluator",
		BranchKind:  "feature",
		Status:      "READY_FOR_REVIEW",
	})
	task := WorkspaceTaskRecord{
		TaskID:      "task-rq-integration",
		Title:       "Integrate rq implementation lanes and publish review-ready candidate",
		Description: "Merge lexer/parser/evaluator/CLI/test lanes and submit one coherent candidate.",
		ProjectID:   "project-alpha",
		ProjectLane: "integration",
		Tags:        []string{"rq", "integration", "review", "patch-queue"},
	}
	if req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination); err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	} else if ok {
		t.Fatalf("integration convergence task must not become owner-bound active-lane publication, got %+v", req)
	}
}

func TestRuntimeOwnerBoundPatchQueueSubmitDoesNotInferAmbiguousOwnerBranch(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	coordination.Branches = append(coordination.Branches, ProjectBranchRecord{
		BranchID:    "branch-gamma-2",
		WorkspaceID: "ws",
		ProjectID:   "project-alpha",
		RepoID:      "repo-main",
		AgentID:     "gamma",
		BranchName:  "agent/gamma/second-submit",
		BranchKind:  "feature",
		Status:      "READY_FOR_REVIEW",
	})
	task := WorkspaceTaskRecord{
		TaskID:    "task-submit-owner-only",
		Title:     "Owner-only project_patch_queue_submit for gamma",
		ProjectID: "project-alpha",
		Tags:      []string{"owner-bound", "owner-bound-kind:patch_queue_submit", "required-agent:gamma"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if !req.RepairNeeded || !strings.Contains(req.Reason, "multiple open branches") {
		t.Fatalf("expected ambiguous owner branches to require repair, got %+v", req)
	}
}

func TestRuntimeOwnerBoundRequirementParsesInlineBranchReference(t *testing.T) {
	rawCoordination, err := json.Marshal(delegatedOwnerBoundProjectCoordination("gamma")["coordination"])
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(rawCoordination, &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-submit-inline",
		Title:       "Owner-only project_patch_queue_submit for gamma",
		Description: "Use branch `agent-gamma` (`branch_id=branch-gamma`, `head_sha=abc`) and patch queue `queue_id=queue-1`, `item_id=item-1`.",
		ProjectID:   "project-alpha",
		Tags:        []string{"patch-queue", "integration", "coordination"},
	}
	req, ok, err := runtimeOwnerBoundRequirementFromCoordination(task, coordination)
	if err != nil {
		t.Fatalf("runtimeOwnerBoundRequirementFromCoordination() error = %v", err)
	}
	if !ok {
		t.Fatal("expected owner-bound requirement")
	}
	if req.RepairNeeded || req.BranchID != "branch-gamma" || req.RequiredAgentID != "gamma" {
		t.Fatalf("expected inline branch_id to resolve branch owner, got %+v", req)
	}
}
