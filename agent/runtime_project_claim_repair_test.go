package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeProjectClaimScopeBusyQueuesStrategicRepair(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRaw(t, "alpha", "beta")
	var methods []string
	var submitParams map[string]any
	var requestParams map[string]any
	var updateParams map[string]any
	var saved RuntimeScratchState
	submitCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			submitCalls++
			submitParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-repair",
				"workspace_id": "ws",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-repair"})
		case "project.coordination.get":
			t.Fatalf("work packet already carries fresh coordination; unexpected project.coordination.get")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	work := projectClaimScopeBusyWork(coordinationRaw)
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), work)
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected project claim busy work to be handled")
	}
	secondWork := projectClaimScopeBusyWork(coordinationRaw)
	secondWork.Packet.ContextHints.AnchorTaskIDs = []string{"task-other-blocked"}
	handled, err = runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), secondWork)
	if err != nil {
		t.Fatalf("second maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected duplicate project claim busy work to be handled")
	}
	if submitCalls != 1 {
		t.Fatalf("expected repair task submit to be idempotent, got %d calls; methods=%+v", submitCalls, methods)
	}
	repairTaskID := rpcString(submitParams, "task_id")
	if !strings.HasPrefix(repairTaskID, "task-project-claim-repair-") {
		t.Fatalf("expected deterministic repair task id, got %+v", submitParams)
	}
	if rpcString(submitParams, "task_kind") != "COORDINATION" || rpcString(submitParams, "project_lane") != "strategy" {
		t.Fatalf("expected strategic coordination repair task, got %+v", submitParams)
	}
	if rpcString(requestParams, "to_agent_id") != "alpha" || rpcString(requestParams, "method") != "model.ask" {
		t.Fatalf("expected wake request to active strategic lead, got %+v", requestParams)
	}
	payloadJSON := rpcString(requestParams, "payload_json")
	if !strings.Contains(payloadJSON, repairTaskID) || !strings.Contains(payloadJSON, `"request_kind":"delegate_task"`) || !strings.Contains(payloadJSON, `"task_id":"`+repairTaskID+`"`) {
		t.Fatalf("expected delegated repair notice payload to reference repair task, got %+v", requestParams)
	}
	if saved.ProjectClaimRepairTaskID != repairTaskID || saved.ProjectClaimRepairRequestID != "req-repair" || strings.TrimSpace(saved.ProjectClaimRepairKey) == "" {
		t.Fatalf("expected scratch to record repair idempotency fields, got %+v", saved)
	}
	updatePayloadJSON := rpcString(updateParams, "payload_json")
	if !strings.Contains(updatePayloadJSON, `"schema":"project_claim_scope_busy_evidence.v1"`) ||
		!strings.Contains(updatePayloadJSON, `"evidence_kind":"project_claim_scope_busy"`) ||
		!strings.Contains(updatePayloadJSON, `"liberation_candidate_kind":"stale_claim_liberation_narrow_candidate"`) ||
		!strings.Contains(updatePayloadJSON, `"blocked_task_id":"task-blocked"`) ||
		!strings.Contains(updatePayloadJSON, `"conflict_owner_agent_id":"beta"`) {
		t.Fatalf("expected repair update payload to describe conflict, got %+v", updateParams)
	}
}

func TestRuntimeProjectClaimScopeBusyDelegatesKnownOwnerBeforeStrategicRepair(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRaw(t, "alpha", "beta")
	var methods []string
	var requestParams map[string]any
	var updateParams map[string]any
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request":
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-owner-resume",
				"workspace_id": "ws",
				"to_agent_id":  "beta",
				"status":       "PENDING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-owner-resume"})
		case "workspace.tasks.list", "task.submit":
			t.Fatalf("known owner handoff should not create strategic repair task first: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	work := projectClaimScopeBusyWork(coordinationRaw)
	work.Packet.PreferredTransition = "delegate_to_branch_owner"
	work.Packet.HandoffToAgentID = "beta"
	work.Packet.Handoff = &AgentWorkHandoff{HandoffState: "branch_owner_required", ToAgentID: "beta"}
	if work.Packet.Gate != nil {
		work.Packet.Gate.NeededFrom = "beta"
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), work)
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected project claim busy work to be handled")
	}
	if rpcString(requestParams, "to_agent_id") != "beta" || rpcString(requestParams, "method") != "model.ask" {
		t.Fatalf("expected owner resume request to beta, got %+v", requestParams)
	}
	payloadJSON := rpcString(requestParams, "payload_json")
	for _, want := range []string{`"schema":"project_claim_owner_resume_request.v1"`, `"request_kind":"project_claim_owner_resume"`, `"evidence_kind":"project_claim_owner_resume_request"`, `"task_id":"task-active"`, `"blocked_task_id":"task-blocked"`, "publish review-ready evidence"} {
		if !strings.Contains(payloadJSON, want) {
			t.Fatalf("expected owner request payload to contain %s, got %+v", want, requestParams)
		}
	}
	if saved.ProjectClaimRepairTaskID != "" || saved.ProjectClaimRepairRequestID != "req-owner-resume" || saved.LastWakeReason != "project_claim_owner_resume" || saved.LastWakeTaskID != "task-active" {
		t.Fatalf("expected scratch to record owner resume, got %+v", saved)
	}
	updatePayloadJSON := rpcString(updateParams, "payload_json")
	if !strings.Contains(updatePayloadJSON, `"schema":"project_claim_scope_busy_evidence.v1"`) ||
		!strings.Contains(updatePayloadJSON, `"release_request_kind":"project_claim_owner_resume"`) ||
		!strings.Contains(updatePayloadJSON, `"release_request_id":"req-owner-resume"`) ||
		!strings.Contains(updatePayloadJSON, `"conflict_owner_agent_id":"beta"`) ||
		!strings.Contains(updatePayloadJSON, "known active owner was asked to resume") {
		t.Fatalf("expected owner resume update payload, got %+v", updateParams)
	}
	if got := strings.Join(methods, ","); got != "agent.request,agent.state.set,agent.update.post" {
		t.Fatalf("unexpected owner resume method order: %s", got)
	}
}

func TestProjectClaimRepairKeyConvergesOnRootConflict(t *testing.T) {
	base := projectClaimRepairConflict{
		ProjectID:              "project-alpha",
		BlockedTaskID:          "task-blocked-a",
		BlockedAgentID:         "agent-gamma",
		BlockedWriteScopeJSON:  `{"paths":["src/ui/**"]}`,
		RepoID:                 "repo-main",
		ConflictKind:           "active_claim",
		ConflictOwnerAgentID:   "agent-beta",
		ConflictTaskID:         "task-owner-a",
		ConflictBranchID:       "branch-beta",
		ConflictBranchStatus:   "ACTIVE",
		ConflictBranchHeadSHA:  "aaaaaaaaaaaa",
		ConflictWriteScopeJSON: `{"paths":["src/**"]}`,
	}
	otherSymptom := base
	otherSymptom.BlockedAgentID = "agent-delta"
	otherSymptom.ConflictKind = "live_branch"
	otherSymptom.ConflictTaskID = "task-owner-b"
	otherSymptom.ConflictBranchStatus = "READY_FOR_REVIEW"
	otherSymptom.ConflictBranchHeadSHA = "bbbbbbbbbbbb"

	if got, want := projectClaimRepairKey("ws", otherSymptom), projectClaimRepairKey("ws", base); got != want {
		t.Fatalf("same root conflict should share repair key, got %q want %q", got, want)
	}

	differentTask := base
	differentTask.BlockedTaskID = "task-blocked-b"
	if got, want := projectClaimRepairKey("ws", differentTask), projectClaimRepairKey("ws", base); got != want {
		t.Fatalf("same root branch conflict should converge across blocked tasks, got %q want %q", got, want)
	}

	differentBranch := base
	differentBranch.ConflictBranchID = "branch-other"
	if got, blocked := projectClaimRepairKey("ws", differentBranch), projectClaimRepairKey("ws", base); got == blocked {
		t.Fatalf("different root conflict branches should produce distinct repair keys")
	}

	unknownA := projectClaimRepairConflict{
		ProjectID:             "project-alpha",
		BlockedTaskID:         "task-blocked-a",
		BlockedAgentID:        "agent-gamma",
		BlockedWriteScopeJSON: `{"paths":["src/**"]}`,
		RepoID:                "repo-main",
		ConflictKind:          "unknown_overlap",
	}
	unknownSameTaskOtherAgent := unknownA
	unknownSameTaskOtherAgent.BlockedAgentID = "agent-iota"
	if got, want := projectClaimRepairKey("ws", unknownSameTaskOtherAgent), projectClaimRepairKey("ws", unknownA); got != want {
		t.Fatalf("unknown overlaps for the same blocked task/scope should dedupe across blocked agents, got %q want %q", got, want)
	}
	unknownB := unknownA
	unknownB.BlockedTaskID = "task-blocked-b"
	if got, blocked := projectClaimRepairKey("ws", unknownB), projectClaimRepairKey("ws", unknownA); got == blocked {
		t.Fatalf("unknown overlaps without concrete owner should remain distinct by blocked task")
	}
}

func TestProjectClaimRepairTaskMatchesRootConflictRequiresRepo(t *testing.T) {
	conflict := projectClaimRepairConflict{
		ProjectID:              "project-alpha",
		RepoID:                 "repo-main",
		ConflictBranchID:       "branch-beta",
		ConflictWriteScopeJSON: `{"paths":["src/ui/**"]}`,
	}
	matching := WorkspaceTaskRecord{
		ProjectID: "project-alpha",
		Title:     "Repair project claim scope conflict",
		Description: strings.Join([]string{
			"- repo_id: repo-main",
			"- conflict_branch_id: branch-beta",
			`- conflict_write_scope_json: {"paths":["src/ui/**"]}`,
		}, "\n"),
	}
	if !projectClaimRepairTaskMatchesRootConflict(matching, conflict) {
		t.Fatalf("expected same repo/root conflict repair to match")
	}

	differentScope := matching
	differentScope.Description = strings.ReplaceAll(differentScope.Description, "src/ui/**", "src/server/**")
	if !projectClaimRepairTaskMatchesRootConflict(differentScope, conflict) {
		t.Fatalf("same repo/root conflict repair should suppress duplicate repair even when the symptom scope differs")
	}

	otherRepo := matching
	otherRepo.Description = strings.ReplaceAll(otherRepo.Description, "repo-main", "repo-other")
	if projectClaimRepairTaskMatchesRootConflict(otherRepo, conflict) {
		t.Fatalf("same branch/scope in another repo must not suppress this repair")
	}

	noRepo := matching
	noRepo.Description = strings.ReplaceAll(noRepo.Description, "- repo_id: repo-main\n", "")
	if projectClaimRepairTaskMatchesRootConflict(noRepo, conflict) {
		t.Fatalf("missing repo_id must not suppress repo-scoped root repair")
	}
}

func TestProjectClaimRepairBlockedTaskFromWorkUsesCoordinationTaskDigest(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRawWithBlockedTasks(t, "alpha", []WorkspaceTaskRecord{
		{
			TaskID:               "task-blocked",
			ProjectID:            "project-alpha",
			ProjectLane:          "implementation",
			TaskKind:             "EXECUTION",
			Status:               "PENDING",
			RequiresProjectGate:  boolPtr(true),
			WriteScopeHints:      []string{"src/**", "public/**"},
			TaskRequirementsJSON: `{"schema":"task_requirements.v1","write_scope_hints":["src/**","public/**"]}`,
		},
	})
	task := projectClaimRepairBlockedTaskFromWork(projectClaimScopeBusyWork(coordinationRaw))
	if task.TaskID != "task-blocked" || strings.Join(task.WriteScopeHints, ",") != "src/**,public/**" {
		t.Fatalf("expected blocked task digest from coordination with write scope hints, got %+v", task)
	}
}

func TestRuntimeProjectClaimRepairDuplicateTaskReusesAndDelegatesLead(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRaw(t, "alpha", "beta")
	var methods []string
	var updateParams map[string]any
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			writeRPCError(w, req, -32000, "workspace task already exists")
		case "agent.request":
			t.Fatalf("duplicate repair task should not re-send strategic lead request: %+v", req.Params)
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-reused"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), projectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected duplicate repair task work to be handled")
	}
	if saved.ProjectClaimRepairTaskID == "" || saved.ProjectClaimRepairRequestID != "" {
		t.Fatalf("expected scratch to record reused repair task without duplicate request id, got %+v", saved)
	}
	payload := rpcString(updateParams, "payload_json")
	for _, want := range []string{`"repair_identity_scope":"root_conflict"`, `"coverage_state":"repair_episode_open"`, `"blocked_task_id":"task-blocked"`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("expected repair reuse payload to contain %s, got %+v", want, updateParams)
		}
	}
	if !strings.Contains(payload, "strategic lead wake request was not re-sent") {
		t.Fatalf("expected duplicate repair payload to mention suppressed wake request, got %+v", updateParams)
	}
	if got := strings.Join(methods, ","); got != "workspace.tasks.list,task.submit,agent.state.set,agent.update.post" {
		t.Fatalf("unexpected duplicate repair method order: %s", got)
	}
}

func TestRuntimeProjectClaimRepairReusesOpenBlockedTaskRepair(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRaw(t, "alpha", "beta")
	var methods []string
	var updateParams map[string]any
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":       "task-project-claim-repair-existing",
				"title":         "Repair project claim scope conflict",
				"description":   "- blocked_task_id: task-blocked\n- conflict_owner_agent_id: beta",
				"status":        "PENDING",
				"task_kind":     "COORDINATION",
				"task_template": "generic",
				"project_id":    "project-alpha",
				"project_lane":  "strategy",
				"tags":          []any{"project-claim-repair", "strategic-lead"},
			}}})
		case "task.submit":
			t.Fatalf("existing repair task for blocked task should be reused, not recreated: %+v", req.Params)
		case "agent.request":
			t.Fatalf("existing repair task should not re-send strategic lead request: %+v", req.Params)
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-existing-repair"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), projectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected open repair reuse to be handled")
	}
	if saved.ProjectClaimRepairTaskID != "task-project-claim-repair-existing" || saved.ProjectClaimRepairRequestID != "" {
		t.Fatalf("expected scratch to record existing repair task without duplicate request, got %+v", saved)
	}
	payloadJSON := rpcString(updateParams, "payload_json")
	if !strings.Contains(payloadJSON, "strategic lead wake request was not re-sent") {
		t.Fatalf("expected existing repair update to mention suppressed wake request, got %+v", updateParams)
	}
	if got := strings.Join(methods, ","); got != "workspace.tasks.list,agent.state.set,agent.update.post" {
		t.Fatalf("unexpected existing repair method order: %s", got)
	}
}

func TestRuntimeProjectClaimRepairSameAgentQueuesResume(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRaw(t, "alpha", "gamma")
	var methods []string
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			if got := rpcString(req.Params, "task_id"); got != "task-active" {
				t.Fatalf("hydrate task_id = %q, want task-active", got)
			}
			writeRPCResult(w, req, delegatedHydrationBundle("task-active", "RUNNING", "gamma", "CLAIMED"))
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update_id": "upd-same-agent"})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "task.submit", "agent.request":
			t.Fatalf("same-agent conflict should resume local owner lane, not submit/delegate repair: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), projectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected same-agent project claim busy work to be handled")
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-active" {
		t.Fatalf("expected same-agent conflict to queue request_resume for active task, got %+v", saved)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.update.post,agent.state.set" {
		t.Fatalf("unexpected same-agent method order: %s", got)
	}
}

func TestRuntimeProjectClaimRepairSameAgentTerminalOwnerResumesBlockedTask(t *testing.T) {
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(mustProjectClaimRepairCoordinationRaw(t, "alpha", "gamma"), &coordination); err != nil {
		t.Fatalf("decode coordination: %v", err)
	}
	for i := range coordination.Tasks {
		if coordination.Tasks[i].TaskID == "task-active" {
			coordination.Tasks[i].Status = "RESOLVED"
			coordination.Tasks[i].TaskKind = "COORDINATION"
		}
	}
	coordination.Tasks = append(coordination.Tasks, WorkspaceTaskRecord{
		TaskID:              "task-blocked",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		TaskKind:            "EXECUTION",
		Status:              "PENDING",
		RequiresProjectGate: boolPtr(true),
		WriteScopeHints:     []string{"src/**"},
	})
	coordinationRaw, err := json.Marshal(coordination)
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}

	var methods []string
	var saved RuntimeScratchState
	var updateParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.task.hydrate":
			switch got := rpcString(req.Params, "task_id"); got {
			case "task-active":
				writeRPCResult(w, req, delegatedHydrationBundle("task-active", "RESOLVED", "gamma", "COMPLETED"))
			case "task-blocked":
				writeRPCResult(w, req, delegatedHydrationBundle("task-blocked", "PENDING", "", ""))
			default:
				t.Fatalf("unexpected hydrate task_id = %q", got)
			}
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-same-agent-terminal"})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "workspace.tasks.list", "task.submit", "agent.request":
			t.Fatalf("terminal same-agent owner should resume blocked product task, not create/delegate repair: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), projectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected same-agent terminal-owner project claim busy work to be handled")
	}
	if saved.PendingTrigger != "request_resume" || saved.PendingTriggerTask != "task-blocked" {
		t.Fatalf("expected terminal owner to resume blocked product task, got %+v", saved)
	}
	if !strings.Contains(rpcString(updateParams, "summary"), "resuming the blocked product lane") {
		t.Fatalf("expected blocked product resume update, got %+v", updateParams)
	}
	if got := strings.Join(methods, ","); got != "agent.task.hydrate,agent.task.hydrate,agent.update.post,agent.state.set" {
		t.Fatalf("unexpected terminal same-agent method order: %s", got)
	}
}

func TestRuntimeProjectClaimRepairPrefersHintedBranchAndSkipsSameAgentEmptyBranch(t *testing.T) {
	for _, tc := range []struct {
		name             string
		anchorBranchIDs  []string
		expectedBranchID string
	}{
		{
			name:             "hinted_beta_branch",
			anchorBranchIDs:  []string{"branch-beta-ready"},
			expectedBranchID: "branch-beta-ready",
		},
		{
			name:             "no_hint_skips_same_agent_empty_branch",
			anchorBranchIDs:  nil,
			expectedBranchID: "branch-beta-ready",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coordinationRaw := mustProjectClaimRepairCoordinationRawWithStaleSameAgentBranch(t)
			var updateParams map[string]any

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeRPCRequest(t, r)
				switch req.Method {
				case "workspace.tasks.list":
					writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
				case "task.submit":
					writeRPCResult(w, req, map[string]any{
						"task_id":      rpcString(req.Params, "task_id"),
						"workspace_id": "ws",
						"status":       "PENDING",
					})
				case "agent.request":
					writeRPCResult(w, req, map[string]any{
						"request_id":   "req-repair",
						"workspace_id": "ws",
						"to_agent_id":  "alpha",
						"status":       "PENDING",
					})
				case "agent.state.set":
					writeRPCResult(w, req, nil)
				case "agent.update.post":
					updateParams = req.Params
					writeRPCResult(w, req, map[string]any{"update_id": "upd-repair"})
				default:
					t.Fatalf("unexpected method %q", req.Method)
				}
			}))
			defer server.Close()

			runtime := &Runtime{
				cfg: RuntimeConfig{
					WorkspaceID: "ws",
					AgentID:     "gamma",
					OwnerUserID: "owner-1",
				},
				client:           NewRhizomeClient(server.URL, "token"),
				eventWakePlanner: make(chan struct{}, 1),
				scratch: RuntimeScratchState{
					DocSHAs: map[string]string{},
				},
			}
			work := projectClaimScopeBusyWork(coordinationRaw)
			work.Packet.ContextHints.AnchorBranchIDs = tc.anchorBranchIDs
			handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), work)
			if err != nil {
				t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
			}
			if !handled {
				t.Fatalf("expected project claim busy work to be handled")
			}
			payload := rpcString(updateParams, "payload_json")
			if !strings.Contains(payload, `"conflict_owner_agent_id":"beta"`) || !strings.Contains(payload, `"conflict_branch_id":"`+tc.expectedBranchID+`"`) {
				t.Fatalf("expected repair conflict to target beta branch, got %+v", updateParams)
			}
			if !strings.Contains(payload, `"conflict_patch_state":"ACCEPTED"`) || !strings.Contains(payload, `"conflict_branch_head_sha":"bbbbbbbbbbbb"`) {
				t.Fatalf("expected repair payload to include beta branch and patch queue evidence, got %+v", updateParams)
			}
			if strings.Contains(payload, `"conflict_branch_id":"branch-gamma-empty"`) {
				t.Fatalf("stale same-agent empty branch must not be selected as conflict owner, got %+v", updateParams)
			}
		})
	}
}

func TestRuntimeProjectClaimRepairUsesAdmissionReasonLiveBranchHint(t *testing.T) {
	requiresGate := true
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			Title:       "Project Alpha",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "IMPLEMENTATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:      "role-lead",
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			AgentID:     "alpha",
			RoleType:    "strategic_lead",
			Status:      "ACTIVE",
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:         "role-gamma",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				AgentID:        "gamma",
				RoleType:       "implementer",
				Status:         "ACTIVE",
				WriteScopeJSON: `{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`,
			},
		},
		Repositories: []ProjectRepositoryRecord{
			{
				RepoID:        "repo-main",
				WorkspaceID:   "ws",
				ProjectID:     "project-alpha",
				DefaultBranch: "main",
				RepoStatus:    "READY",
				IsCanonical:   true,
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-generic-first",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        "beta",
				ActiveTaskID:   "task-generic-first",
				BranchName:     "agent/beta/project-alpha/task-generic-first",
				WriteScopeJSON: `{"paths":["cmd/**"]}`,
				Status:         "ACTIVE",
			},
			{
				BranchID:       "branch-from-admission",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        "theta",
				ActiveTaskID:   "task-from-admission",
				BranchName:     "agent/theta/project-alpha/task-from-admission",
				HeadSHA:        "abcdef123456",
				ReviewDocKey:   "project.project-alpha.branch.branch-from-admission.review",
				WriteScopeJSON: `{"paths":["cmd/**","internal/cli/**"]}`,
				Status:         "READY_FOR_REVIEW",
			},
		},
	}
	coordinationRaw, err := json.Marshal(coordination)
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	work := projectClaimScopeBusyWork(coordinationRaw)
	task := WorkspaceTaskRecord{
		TaskID:              "task-blocked",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		TaskKind:            "EXECUTION",
		Status:              "PENDING",
		RequiresProjectGate: &requiresGate,
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
	}

	conflict, _, err := runtime.buildProjectClaimRepairConflict(
		context.Background(),
		task,
		&work,
		"rpc agent.task.claim: task claim project admission invalid: write_scope_json overlaps live branch_id=branch-from-admission active_task_id=task-from-admission",
	)
	if err != nil {
		t.Fatalf("build repair conflict: %v", err)
	}
	if conflict.ConflictKind != "live_branch" ||
		conflict.ConflictBranchID != "branch-from-admission" ||
		conflict.ConflictTaskID != "task-from-admission" ||
		conflict.ConflictOwnerAgentID != "theta" {
		t.Fatalf("expected admission diagnostic to select exact live branch conflict, got %+v", conflict)
	}
	if conflict.ConflictBranchHeadSHA != "abcdef123456" || conflict.ConflictReviewDocKey != "project.project-alpha.branch.branch-from-admission.review" {
		t.Fatalf("expected live branch evidence to be preserved, got %+v", conflict)
	}
}

func TestRuntimeProjectClaimScopeBusyMissingAnchorInfersSingleTask(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRawWithBlockedTasks(t, "alpha", []WorkspaceTaskRecord{
		projectClaimRepairBlockedTask("task-blocked"),
	})
	var submitParams map[string]any
	var requestParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			submitParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			requestParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-repair",
				"workspace_id": "ws",
				"to_agent_id":  "alpha",
				"status":       "PENDING",
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-repair"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), malformedProjectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected malformed project claim busy work to be handled")
	}
	if repairTaskID := rpcString(submitParams, "task_id"); !strings.HasPrefix(repairTaskID, "task-project-claim-repair-") {
		t.Fatalf("expected inferred repair task submit, got %+v", submitParams)
	}
	if !strings.Contains(rpcString(submitParams, "description"), "blocked_task_id: task-blocked") ||
		!strings.Contains(rpcString(submitParams, "description"), "malformed_work_next_packet_missing_anchor=true") {
		t.Fatalf("expected repair description to identify inferred malformed packet, got %+v", submitParams)
	}
	if rpcString(requestParams, "to_agent_id") != "alpha" {
		t.Fatalf("expected strategic lead wake, got %+v", requestParams)
	}
	if !strings.Contains(rpcString(updateParams, "payload_json"), `"blocked_task_id":"task-blocked"`) {
		t.Fatalf("expected repair update payload to describe inferred blocked task, got %+v", updateParams)
	}
}

func TestRuntimeProjectClaimScopeBusyMissingAnchorAmbiguousPostsIssue(t *testing.T) {
	coordinationRaw := mustProjectClaimRepairCoordinationRawWithBlockedTasks(t, "alpha", []WorkspaceTaskRecord{
		projectClaimRepairBlockedTask("task-blocked-a"),
		projectClaimRepairBlockedTask("task-blocked-b"),
	})
	var updateParams map[string]any
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, map[string]any{"update_id": "upd-malformed"})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "task.submit", "agent.request":
			t.Fatalf("ambiguous malformed packet must not guess repair task: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
			OwnerUserID: "owner-1",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	handled, err := runtime.maybeEnqueueProjectClaimRepairFromWork(context.Background(), malformedProjectClaimScopeBusyWork(coordinationRaw))
	if err != nil {
		t.Fatalf("maybeEnqueueProjectClaimRepairFromWork() error = %v", err)
	}
	if !handled {
		t.Fatalf("expected malformed project claim busy work to suppress generic idle reflection")
	}
	if got := strings.Join(methods, ","); got != "agent.update.post,agent.state.set" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if rpcString(updateParams, "update_type") != "issue" || !strings.Contains(rpcString(updateParams, "payload_json"), `"malformed_work_next_packet_missing_anchor":true`) {
		t.Fatalf("expected malformed packet issue update, got %+v", updateParams)
	}
	if !strings.Contains(rpcString(updateParams, "summary"), "2 candidate implementation tasks") {
		t.Fatalf("expected ambiguous candidate count in summary, got %+v", updateParams)
	}
}

func projectClaimScopeBusyWork(coordinationRaw json.RawMessage) AgentWorkNextResult {
	requiresGate := true
	return AgentWorkNextResult{
		WorkspaceID:                "ws",
		AgentID:                    "gamma",
		Reason:                     "project_claim_scope_busy",
		ProjectID:                  "project-alpha",
		TaskKind:                   "EXECUTION",
		ProjectLane:                "implementation",
		RequiresProjectGate:        &requiresGate,
		ProjectCoordination:        coordinationRaw,
		AutonomousExecutionAllowed: true,
		Packet: &AgentWorkPacket{
			WorkType:            "project_claim_scope_busy",
			CoordinationState:   "project_claim_scope_busy",
			PreferredTransition: "request_strategic_repair",
			ProjectID:           "project-alpha",
			TaskKind:            "EXECUTION",
			ProjectLane:         "implementation",
			RequiresProjectGate: &requiresGate,
			ProjectCoordination: coordinationRaw,
			ContextHints: AgentWorkContextHints{
				AnchorTaskIDs: []string{"task-blocked"},
			},
		},
	}
}

func malformedProjectClaimScopeBusyWork(coordinationRaw json.RawMessage) AgentWorkNextResult {
	work := projectClaimScopeBusyWork(coordinationRaw)
	work.Packet.ContextHints = AgentWorkContextHints{}
	return work
}

func mustProjectClaimRepairCoordinationRaw(t *testing.T, leadAgentID, conflictOwnerAgentID string) json.RawMessage {
	t.Helper()
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			Title:       "Project Alpha",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "IMPLEMENTATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:      "role-lead",
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			AgentID:     leadAgentID,
			RoleType:    "strategic_lead",
			Status:      "ACTIVE",
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:         "role-gamma",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				AgentID:        "gamma",
				RoleType:       "implementer",
				Status:         "ACTIVE",
				WriteScopeJSON: `{"paths":["src/**"]}`,
			},
		},
		Repositories: []ProjectRepositoryRecord{
			{
				RepoID:        "repo-main",
				WorkspaceID:   "ws",
				ProjectID:     "project-alpha",
				DefaultBranch: "main",
				RepoStatus:    "READY",
				IsCanonical:   true,
			},
		},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:              "task-active",
				ProjectID:           "project-alpha",
				ProjectLane:         "implementation",
				Status:              "RUNNING",
				ClaimAgentID:        stringPtr(conflictOwnerAgentID),
				ClaimStatus:         stringPtr("CLAIMED"),
				ClaimRepoID:         stringPtr("repo-main"),
				ClaimBranchID:       stringPtr("branch-active"),
				ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**"]}`),
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-active",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        conflictOwnerAgentID,
				ActiveTaskID:   "task-active",
				BranchName:     "agent/" + conflictOwnerAgentID + "/project-alpha/task-active",
				WriteScopeJSON: `{"paths":["src/**"]}`,
				Status:         "ACTIVE",
			},
		},
	}
	raw, err := json.Marshal(coordination)
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	return raw
}

func mustProjectClaimRepairCoordinationRawWithStaleSameAgentBranch(t *testing.T) json.RawMessage {
	t.Helper()
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			Title:       "Project Alpha",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "IMPLEMENTATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:      "role-lead",
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			AgentID:     "alpha",
			RoleType:    "strategic_lead",
			Status:      "ACTIVE",
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:         "role-gamma",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				AgentID:        "gamma",
				RoleType:       "implementer",
				Status:         "ACTIVE",
				WriteScopeJSON: `{"paths":["src/**"]}`,
			},
		},
		Repositories: []ProjectRepositoryRecord{
			{
				RepoID:        "repo-main",
				WorkspaceID:   "ws",
				ProjectID:     "project-alpha",
				DefaultBranch: "main",
				RepoStatus:    "READY",
				IsCanonical:   true,
			},
		},
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-gamma-empty",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        "gamma",
				BranchName:     "agent/gamma/project-alpha/empty",
				WriteScopeJSON: `{"paths":["src/**"]}`,
				Status:         "ACTIVE",
			},
			{
				BranchID:       "branch-beta-ready",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        "beta",
				BranchName:     "agent/beta/project-alpha/scaffold",
				HeadSHA:        "bbbbbbbbbbbb",
				ReviewDocKey:   "project.project-alpha.branch.branch-beta-ready.review",
				WriteScopeJSON: `{"paths":["src/**"]}`,
				Status:         "READY_FOR_REVIEW",
			},
		},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:         "patchq-project-alpha",
				ItemID:          "patchitem-branch-beta-ready",
				WorkspaceID:     "ws",
				ProjectID:       "project-alpha",
				RepoID:          "repo-main",
				BranchID:        "branch-beta-ready",
				ReviewDocKey:    "project.project-alpha.branch.branch-beta-ready.review",
				State:           "ACCEPTED",
				HeadSHA:         "bbbbbbbbbbbb",
				DecisionSummary: "Accepted scaffold branch before downstream preview/export.",
			},
		},
	}
	raw, err := json.Marshal(coordination)
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	return raw
}

func mustProjectClaimRepairCoordinationRawWithBlockedTasks(t *testing.T, leadAgentID string, blockedTasks []WorkspaceTaskRecord) json.RawMessage {
	t.Helper()
	trueValue := true
	tasks := append([]WorkspaceTaskRecord(nil), blockedTasks...)
	tasks = append(tasks, WorkspaceTaskRecord{
		TaskID:              "task-active",
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		TaskKind:            "EXECUTION",
		Status:              "RUNNING",
		RequiresProjectGate: &trueValue,
		ClaimAgentID:        stringPtr("beta"),
		ClaimStatus:         stringPtr("CLAIMED"),
		ClaimRepoID:         stringPtr("repo-main"),
		ClaimBranchID:       stringPtr("branch-active"),
		ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**"]}`),
	})
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			Title:       "Project Alpha",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:  "ws",
			ProjectID:    "project-alpha",
			CurrentPhase: "IMPLEMENTATION",
			RepoRequired: true,
			RepoStatus:   "READY",
		},
		StrategicLead: &ProjectRoleRecord{
			RoleID:      "role-lead",
			WorkspaceID: "ws",
			ProjectID:   "project-alpha",
			AgentID:     leadAgentID,
			RoleType:    "strategic_lead",
			Status:      "ACTIVE",
		},
		Roles: []ProjectRoleRecord{
			{
				RoleID:         "role-gamma",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				AgentID:        "gamma",
				RoleType:       "implementer",
				Status:         "ACTIVE",
				WriteScopeJSON: `{"paths":["src/**"]}`,
			},
		},
		Repositories: []ProjectRepositoryRecord{
			{
				RepoID:        "repo-main",
				WorkspaceID:   "ws",
				ProjectID:     "project-alpha",
				DefaultBranch: "main",
				RepoStatus:    "READY",
				IsCanonical:   true,
			},
		},
		Tasks: tasks,
		Branches: []ProjectBranchRecord{
			{
				BranchID:       "branch-active",
				WorkspaceID:    "ws",
				ProjectID:      "project-alpha",
				RepoID:         "repo-main",
				AgentID:        "beta",
				ActiveTaskID:   "task-active",
				BranchName:     "agent/beta/project-alpha/task-active",
				WriteScopeJSON: `{"paths":["src/**"]}`,
				Status:         "READY_FOR_REVIEW",
			},
		},
	}
	raw, err := json.Marshal(coordination)
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}
	return raw
}

func projectClaimRepairBlockedTask(taskID string) WorkspaceTaskRecord {
	trueValue := true
	return WorkspaceTaskRecord{
		TaskID:              taskID,
		ProjectID:           "project-alpha",
		ProjectLane:         "implementation",
		TaskKind:            "EXECUTION",
		Status:              "PENDING",
		RequiresProjectGate: &trueValue,
		ClaimAgentID:        stringPtr("gamma"),
		ClaimStatus:         stringPtr("RELEASED"),
		ClaimRepoID:         stringPtr("repo-main"),
		ClaimWriteScopeJSON: stringPtr(`{"paths":["src/**"]}`),
	}
}
