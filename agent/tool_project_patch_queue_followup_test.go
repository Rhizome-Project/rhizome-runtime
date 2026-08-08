package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectPatchQueueIntegrationFollowupCarriesFullProductBoundary(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:         "queue-1",
		ItemID:          "item-1",
		RepoID:          "repo-1",
		BranchID:        "branch-lexer",
		State:           "ACCEPTED",
		HeadSHA:         strings.Repeat("a", 40),
		Pathset:         []string{"internal/lexer/**", "go.mod"},
		DecisionSummary: "Lexer lane accepted for integration.",
	}
	requirements := projectPatchQueueFollowupTaskRequirements(item, "integration")
	if requirements["required_tool"] != "project_patch_queue_integrate" ||
		requirements["required_transition"] != "project_patch_queue_integrate_then_full_product_verify" ||
		requirements["integration_boundary"] != "accepted_lane_candidate_must_be_assembled_before_full_product_acceptance" {
		t.Fatalf("integration requirements should preserve full-product boundary, got %+v", requirements)
	}
	description := renderProjectPatchQueueFollowupDescription(item, "integration", "")
	for _, want := range []string{
		"full-product assembly boundary",
		"Lane ACCEPTED evidence is necessary but not sufficient",
		"verifier/review mesh",
		"cross-lane bugs should be caught",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("integration follow-up missing %q:\n%s", want, description)
		}
	}
}

func TestProjectPatchQueueIntegrationFollowupAuthorizesDirectMergeForAcceptedControlledItemWithoutMaterialization(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:           "queue-1",
		ItemID:            "item-1",
		RepoID:            "repo-1",
		BranchID:          "branch-lexer",
		State:             "ACCEPTED",
		HeadSHA:           strings.Repeat("a", 40),
		Pathset:           []string{"internal/lexer/**"},
		DecisionSummary:   "Lexer lane accepted for integration.",
		RepoAuthorityMode: "repoauthority_controlled_queue",
	}
	requirements := projectPatchQueueFollowupTaskRequirements(item, "integration")
	if requirements["required_tool"] != "project_patch_queue_integrate" ||
		requirements["integration_mode"] != "direct_merge" ||
		requirements["integration_authorization"] != "direct_merge_for_accepted_unmaterialized_controlled_queue" ||
		requirements["controlled_queue_materialization_state"] != "missing_before_acceptance" {
		t.Fatalf("controlled accepted integration task should carry direct_merge authorization, got %+v", requirements)
	}
	args, ok := requirements["required_tool_args"].(map[string]any)
	if !ok || args["integration_mode"] != "direct_merge" {
		t.Fatalf("controlled accepted integration task should carry required_tool_args integration_mode=direct_merge, got %#v", requirements["required_tool_args"])
	}
	description := renderProjectPatchQueueFollowupDescription(item, "integration", "")
	if !strings.Contains(description, "integration_mode=direct_merge") ||
		!strings.Contains(description, "impossible post-ACCEPTED materialization") {
		t.Fatalf("integration follow-up should explain direct_merge route:\n%s", description)
	}
}

func TestProjectPatchQueueRevisionFollowupCarriesPublicationLadder(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:         "queue-1",
		ItemID:          "item-1",
		RepoID:          "repo-1",
		BranchID:        "branch-lexer",
		State:           "BLOCKED",
		HeadSHA:         strings.Repeat("b", 40),
		Pathset:         []string{"internal/lexer/**", "go.mod"},
		DecisionSummary: "Lexer lane has same-head defect evidence.",
	}
	requirements := projectPatchQueueFollowupTaskRequirements(item, "revision")
	if requirements["required_transition"] != "project_patch_queue_revision_commit_review_submit" ||
		requirements["required_first_publication_tool"] != "project_branch_commit" ||
		requirements["required_terminal_tool"] != "project_patch_queue_submit" ||
		requirements["historical_source_branch_role"] != "read_only_defeated_source_branch_evidence" ||
		requirements["live_repair_branch_required"] != true {
		t.Fatalf("revision requirements should carry durable publication ladder, got %+v", requirements)
	}
	sequence, ok := requirements["required_tool_sequence"].([]string)
	if !ok || strings.Join(sequence, ",") != "project_branch_commit,project_branch_review_ready,project_patch_queue_submit" {
		t.Fatalf("unexpected revision tool sequence: %#v", requirements["required_tool_sequence"])
	}
	if _, ok := requirements["write_scope_hints"]; ok {
		t.Fatalf("revision requirements must not turn candidate pathset into write scope: %+v", requirements)
	}
	description := renderProjectPatchQueueFollowupDescription(item, "revision", "")
	for _, want := range []string{
		"read-only historical evidence",
		"project_branch_commit with push=true",
		"project_branch_review_ready",
		"project_patch_queue_submit",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("revision follow-up missing %q:\n%s", want, description)
		}
	}
}

func TestProjectPatchQueueFollowupToolCreatesIntegrationTaskForAcceptedItem(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":               "ACCEPTED",
				"claimed_by":          "agent-alpha",
				"claim_token":         "claim-token-1",
				"decision_summary":    "Accepted after integration review; verify browser smoke and artifact export.",
				"decision_doc_key":    "project.project-subpixel.patchq.decision",
				"decided_by":          "agent-alpha",
				"decided_at":          "2026-04-28T12:01:00Z",
				"pathset":             []string{"web/app.js", "web/index.html"},
				"repo_authority_mode": "patch_only_temp_repo",
			}))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			expectedTaskID := projectPatchQueueFollowupTaskID("project-subpixel", ProjectPatchQueueItemRecord{
				QueueID: "patchq-project-subpixel-projrepo-1",
				ItemID:  "patchitem-branch-1",
			}, "integration")
			if got := rpcString(req.Params, "task_id"); got != expectedTaskID {
				t.Fatalf("task_id = %q", got)
			}
			for key, want := range map[string]string{
				"project_id":   "project-subpixel",
				"project_lane": "integration",
				"task_kind":    "EXECUTION",
				"priority":     "high",
				"linked_by":    "agent-beta",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got, ok := req.Params["requires_project_gate"].(bool); !ok || !got {
				t.Fatalf("requires_project_gate = %#v, want true", req.Params["requires_project_gate"])
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "web/app.js,web/index.html" {
				t.Fatalf("write_scope_hints = %q", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{
				"Decision summary:",
				"verify browser smoke",
				"review_doc_key: project.project-subpixel.branch.branch-1.review",
				"decision_doc_key: project.project-subpixel.patchq.decision",
				"head_sha: head123",
				"project_patch_queue_integrate",
				"acceptance alone is not a canonical baseline update",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("description missing %q:\n%s", want, description)
				}
			}
			tags := strings.Join(rpcStringSlice(req.Params, "tags"), ",")
			for _, want := range []string{"project", "patch-queue", "integration", "accepted"} {
				if !strings.Contains(tags, want) {
					t.Fatalf("tags missing %q: %s", want, tags)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			for _, want := range []string{"- project_lane: integration", "- requires_project_gate: true", "- write_scope_hints: web/app.js, web/index.html"} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical doc missing %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful followup, got %+v", result)
	}
	for _, want := range []string{"integration", "task_submit_result", "task-patchq-integration-project-subpixel", "no_git_mutation"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolPrefersAcceptedBranchHeadOverOldBlockedItem(t *testing.T) {
	oldHead := strings.Repeat("1", 40)
	newHead := strings.Repeat("2", 40)
	oldItem := patchQueueLifecycleItem(map[string]any{
		"queue_id":            "patchq-project-subpixel-projrepo-1-old",
		"item_id":             "patchitem-branch-1-old",
		"state":               "BLOCKED",
		"head_sha":            oldHead,
		"decision_summary":    "Old candidate blocked pending browser evidence.",
		"decision_doc_key":    "project.project-subpixel.patchq.old-blocked",
		"review_doc_key":      "project.project-subpixel.branch.branch-1.old-review",
		"pathset":             []string{"web/old.js"},
		"repo_authority_mode": "patch_only_temp_repo",
	})
	newItem := patchQueueLifecycleItem(map[string]any{
		"queue_id":            "patchq-project-subpixel-projrepo-1-new",
		"item_id":             "patchitem-branch-1-new",
		"state":               "ACCEPTED",
		"head_sha":            newHead,
		"decision_summary":    "Fresh accepted candidate supersedes the old blocked head.",
		"decision_doc_key":    "project.project-subpixel.patchq.new-accepted",
		"review_doc_key":      "project.project-subpixel.branch.branch-1.new-review",
		"pathset":             []string{"web/new.js"},
		"repo_authority_mode": "patch_only_temp_repo",
	})
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueCoordinationResult([]map[string]any{oldItem, newItem})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{{
				"task_id":       "task-old-terminal-integration-followup",
				"title":         "Integrate accepted candidate branch-1",
				"description":   "Patch queue decision follow-up.\n\n- queue_id: patchq-project-subpixel-projrepo-1-old\n- item_id: patchitem-branch-1-old\n- branch_id: branch-1\n- head_sha: " + oldHead + "\n- state: BLOCKED",
				"owner_user_id": "owner-1",
				"priority":      "high",
				"status":        "RESOLVED",
				"task_kind":     "EXECUTION",
				"project_id":    "project-subpixel",
				"project_lane":  "integration",
				"tags":          []string{"project", "patch-queue", "integration", "blocked"},
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "integration" {
				t.Fatalf("project_lane = %q, want integration", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{
				"queue_id: patchq-project-subpixel-projrepo-1-new",
				"item_id: patchitem-branch-1-new",
				"head_sha: " + newHead,
				"project_patch_queue_integrate",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("fresh accepted follow-up description missing %q:\n%s", want, description)
				}
			}
			for _, old := range []string{"patchitem-branch-1-old", oldHead} {
				if strings.Contains(description, old) {
					t.Fatalf("fresh accepted follow-up must not route old blocked lineage %q:\n%s", old, description)
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "patchitem-branch-1-new") || strings.Contains(content, "patchitem-branch-1-old") {
				t.Fatalf("canonical doc should bind only fresh accepted item:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-new-integration"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"queue_id":   "patchq-project-subpixel-projrepo-1-new",
		"item_id":    "patchitem-branch-1-new",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected fresh accepted follow-up success, got %+v", result)
	}
	for _, want := range []string{`"followup_kind": "integration"`, "task-patchq-integration-project-subpixel", "patchitem-branch-1-new"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "patchitem-branch-1-old") {
		t.Fatalf("result must not point at old blocked item, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestSelectProjectPatchQueueFollowupItemRequiresItemIDForQueueLineage(t *testing.T) {
	items := []ProjectPatchQueueItemRecord{
		{
			QueueID:   "patchq-project-subpixel-projrepo-1",
			ItemID:    "patchitem-old",
			BranchID:  "branch-1",
			State:     "ACCEPTED",
			UpdatedAt: "2026-05-01T00:00:00Z",
		},
		{
			QueueID:   "patchq-project-subpixel-projrepo-1",
			ItemID:    "patchitem-new",
			BranchID:  "branch-1",
			State:     "BLOCKED",
			UpdatedAt: "2026-05-02T00:00:00Z",
		},
	}
	if _, err := selectProjectPatchQueueFollowupItem(items, "patchq-project-subpixel-projrepo-1", "", ""); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("queue-only multi-item selector must fail closed, got %v", err)
	}
	got, err := selectProjectPatchQueueFollowupItem(items, "patchq-project-subpixel-projrepo-1", "patchitem-new", "")
	if err != nil {
		t.Fatalf("exact queue/item selector should resolve: %v", err)
	}
	if got.ItemID != "patchitem-new" {
		t.Fatalf("resolved item = %+v", got)
	}
}

func TestSelectProjectPatchQueueFollowupItemRejectsItemIDOnlyCollision(t *testing.T) {
	items := []ProjectPatchQueueItemRecord{
		{
			QueueID:   "patchq-project-subpixel-projrepo-old",
			ItemID:    "patchitem-shared",
			BranchID:  "branch-old",
			State:     "ACCEPTED",
			UpdatedAt: "2026-05-01T00:00:00Z",
		},
		{
			QueueID:   "patchq-project-subpixel-projrepo-new",
			ItemID:    "patchitem-shared",
			BranchID:  "branch-new",
			State:     "BLOCKED",
			UpdatedAt: "2026-05-02T00:00:00Z",
		},
	}
	if _, err := selectProjectPatchQueueFollowupItem(items, "", "patchitem-shared", ""); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("item_id-only selector collision must fail closed, got %v", err)
	}
	got, err := selectProjectPatchQueueFollowupItem(items, "patchq-project-subpixel-projrepo-new", "patchitem-shared", "")
	if err != nil {
		t.Fatalf("exact queue/item selector should resolve after collision: %v", err)
	}
	if got.QueueID != "patchq-project-subpixel-projrepo-new" || got.ItemID != "patchitem-shared" {
		t.Fatalf("resolved wrong item after exact selector: %+v", got)
	}
}

func TestProjectPatchQueueFollowupToolSkipsTerminalExistingFollowupFromCoordination(t *testing.T) {
	existingTaskID := projectPatchQueueFollowupTaskID("project-subpixel", ProjectPatchQueueItemRecord{
		QueueID: "patchq-project-subpixel-projrepo-1",
		ItemID:  "patchitem-branch-1",
	}, "integration")
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "ACCEPTED",
				"decision_summary": "Accepted and already integrated by an existing follow-up task.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{{
				"task_id":       existingTaskID,
				"title":         "Integrate accepted candidate branch-1",
				"description":   "Already handled integration follow-up.",
				"owner_user_id": "owner-1",
				"priority":      "high",
				"status":        "RESOLVED",
				"task_kind":     "EXECUTION",
				"project_id":    "project-subpixel",
				"project_lane":  "integration",
				"linked_by":     "agent-beta",
				"linked_at":     "2026-05-13T22:00:00Z",
			}}
			writeRPCResult(w, req, result)
		default:
			t.Fatalf("terminal existing follow-up should not submit duplicate task, got method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected terminal existing follow-up no-op, got %+v", result)
	}
	for _, want := range []string{`"created": false`, `"reason": "existing_followup_task_terminal"`, `"existing_status": "RESOLVED"`, "Do not claim or delegate it"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolConvertsDuplicateResolvedTaskToNoop(t *testing.T) {
	existingTaskID := projectPatchQueueFollowupTaskID("project-subpixel", ProjectPatchQueueItemRecord{
		QueueID: "patchq-project-subpixel-projrepo-1",
		ItemID:  "patchitem-branch-1",
	}, "integration")
	calls := make([]string, 0, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "ACCEPTED",
				"decision_summary": "Accepted and already integrated by an existing follow-up task.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
			}))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "task_id"); got != existingTaskID {
				t.Fatalf("task_id = %q, want %q", got, existingTaskID)
			}
			writeRPCError(w, req, -32000, "workspace task already exists: "+existingTaskID)
		case "agent.task.hydrate":
			writeRPCResult(w, req, delegatedProjectHydrationBundle(existingTaskID, "RESOLVED", "", "", "project-subpixel", "integration"))
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected duplicate terminal follow-up to become no-op, got %+v", result)
	}
	for _, want := range []string{`"created": false`, `"reason": "existing_followup_task_terminal"`, "fresh branch/head-bound evidence docs"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "task_result_doc_key") {
		t.Fatalf("terminal no-op should not invent a result doc key, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,agent.task.hydrate" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolCreatesRetryForTerminalBlockedValidationFollowup(t *testing.T) {
	existingTaskID := projectPatchQueueFollowupTaskID("project-subpixel", ProjectPatchQueueItemRecord{
		QueueID: "patchq-project-subpixel-projrepo-1",
		ItemID:  "patchitem-branch-1",
	}, "validation")
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Missing browser validation evidence after the first validation follow-up.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"updated_at":       "2026-05-14T12:00:00Z",
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{{
				"task_id":       existingTaskID,
				"title":         "Validate blocked integration candidate branch-1",
				"description":   "First validation follow-up.",
				"owner_user_id": "owner-1",
				"priority":      "high",
				"status":        "RESOLVED",
				"task_kind":     "EXECUTION",
				"project_id":    "project-subpixel",
				"project_lane":  "validation",
				"linked_by":     "agent-beta",
				"linked_at":     "2026-05-14T12:01:00Z",
				"updated_at":    "2026-05-14T12:02:00Z",
			}}
			writeRPCResult(w, req, result)
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			got := rpcString(req.Params, "task_id")
			if got == existingTaskID || !strings.Contains(got, "retry") {
				t.Fatalf("expected retry task id distinct from base %q, got %q", existingTaskID, got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "validation" {
				t.Fatalf("project_lane = %q, want validation", got)
			}
			if desc := rpcString(req.Params, "description"); !strings.Contains(desc, "retry_of_terminal_followup_task: "+existingTaskID) {
				t.Fatalf("expected retry context in description, got %q", desc)
			}
			writeRPCResult(w, req, map[string]any{"task_id": got, "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"doc_sha": "sha"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected retry follow-up creation, got %+v", result)
	}
	for _, want := range []string{`"followup_kind": "validation"`, `"task_submit_result"`, "retry"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolUsesExistingRandomRevisionInsteadOfDuplicateValidation(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID: "patchq-project-subpixel-projrepo-1",
		ItemID:  "patchitem-branch-1",
	}
	validationTaskID := projectPatchQueueFollowupTaskID("project-subpixel", item, "validation")
	revisionTaskID := "task-random-revision-branch-1"
	calls := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked pending missing manual browser verification and production build evidence.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"updated_at":       "2026-05-14T12:00:00Z",
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{
				{
					"task_id":       validationTaskID,
					"title":         "Validate blocked integration candidate branch-1",
					"owner_user_id": "owner-1",
					"priority":      "high",
					"status":        "RESOLVED",
					"task_kind":     "EXECUTION",
					"project_id":    "project-subpixel",
					"project_lane":  "validation",
					"linked_by":     "agent-epsilon",
					"updated_at":    "2026-05-14T12:02:00Z",
				},
				{
					"task_id":       revisionTaskID,
					"title":         "Unblock integration candidate branch-1",
					"description":   "Patch queue decision follow-up.\n\n- queue_id: patchq-project-subpixel-projrepo-1\n- item_id: patchitem-branch-1\n- branch_id: branch-1\n- state: BLOCKED",
					"owner_user_id": "owner-1",
					"priority":      "high",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"project_id":    "project-subpixel",
					"project_lane":  "implementation",
					"tags":          []string{"project", "patch-queue", "revision", "blocked"},
					"linked_by":     "agent-epsilon",
					"updated_at":    "2026-05-14T12:03:00Z",
				},
			}
			writeRPCResult(w, req, result)
		default:
			t.Fatalf("existing revision should suppress duplicate validation submit, got method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected existing revision no-op, got %+v", result)
	}
	for _, want := range []string{`"created": false`, `"followup_kind": "revision"`, `"existing_task_id": "` + revisionTaskID + `"`, `"existing_status": "PENDING"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, validationTaskID+"-retry") {
		t.Fatalf("result must not point at validation retry task, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "project.coordination.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolTurnsBlockingVisualEvidenceIntoRevision(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	calls := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked only because browser/e2e and visual acceptance evidence were missing.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-05-25T12:00:00Z",
				"head_sha":         headSHA,
			}))
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
			}}})
		case "workspace.doc.get":
			if got := rpcString(req.Params, "doc_key"); got != evidenceDocKey {
				t.Fatalf("unexpected doc get %q", got)
			}
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
				Content: `schema: rhizome_visual_acceptance_v1
queue_id: patchq-project-subpixel-projrepo-1
item_id: patchitem-branch-1
branch_id: branch-1
head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
visual_verdict: fail
severity: blocking`,
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "owner_user_id"); got != "agent-alpha" {
				t.Fatalf("revision follow-up owner_user_id = %q, want branch owner agent-alpha", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if got := rpcString(req.Params, "title"); !strings.Contains(got, "Unblock integration candidate") {
				t.Fatalf("expected revision follow-up title, got %q", got)
			}
			if desc := rpcString(req.Params, "description"); !strings.Contains(desc, evidenceDocKey) || !strings.Contains(desc, "visual_evidence_route") {
				t.Fatalf("revision description missing visual evidence context:\n%s", desc)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- project_lane: implementation") || !strings.Contains(content, evidenceDocKey) {
				t.Fatalf("canonical doc missing revision/evidence metadata:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-revision"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected revision follow-up from blocking visual evidence, got %+v", result)
	}
	for _, want := range []string{`"followup_kind": "revision"`, evidenceDocKey, `"task_submit_result"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.doc.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolTurnsTaskBoundBlockingVisualEvidenceIntoRevision(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	calls := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked only because browser/e2e and visual acceptance evidence were missing.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-05-25T12:00:00Z",
				"head_sha":         headSHA,
			}))
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
			}}})
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
				Content: `# rhizome_visual_acceptance_v1

- task_id: task-clearpress-visual-acceptance-beta-head-4dc89d3c-refresh
- project_id: project-subpixel
- candidate_head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
- candidate_kind: provisional_non_canonical_validation_checkout

## Verdict
- visual_verdict: fail
- severity: blocking`,
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if desc := rpcString(req.Params, "description"); !strings.Contains(desc, evidenceDocKey) || !strings.Contains(desc, "visual_evidence_route") {
				t.Fatalf("revision description missing visual evidence context:\n%s", desc)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-revision"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected revision follow-up from task-bound blocking visual evidence, got %+v", result)
	}
	for _, want := range []string{`"followup_kind": "revision"`, evidenceDocKey, `"task_submit_result"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.doc.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolIgnoresStaleValidationWhenBlockingVisualEvidenceExists(t *testing.T) {
	const headSHA = "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	const evidenceDocKey = "task.task-visual-refresh.visual_acceptance"
	item := ProjectPatchQueueItemRecord{
		QueueID:  "patchq-project-subpixel-projrepo-1",
		ItemID:   "patchitem-branch-1",
		BranchID: "branch-1",
		HeadSHA:  headSHA,
	}
	validationTaskID := projectPatchQueueFollowupTaskID("project-subpixel", item, "validation")
	calls := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked only because browser/e2e and visual acceptance evidence were missing.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-05-25T12:00:00Z",
				"head_sha":         headSHA,
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{{
				"task_id":       validationTaskID,
				"title":         "Validate blocked integration candidate branch-1",
				"description":   "Patch queue validation follow-up.\n\n- queue_id: patchq-project-subpixel-projrepo-1\n- item_id: patchitem-branch-1\n- branch_id: branch-1\n- head_sha: " + headSHA,
				"owner_user_id": "owner-1",
				"priority":      "high",
				"status":        "PENDING",
				"task_kind":     "EXECUTION",
				"project_id":    "project-subpixel",
				"project_lane":  "validation",
				"tags":          []string{"project", "patch-queue", "validation", "browser"},
				"linked_by":     "agent-alpha",
				"updated_at":    "2026-05-14T12:02:00Z",
			}}
			writeRPCResult(w, req, result)
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
			}}})
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c fail",
				Content: `schema: rhizome_visual_acceptance_v1
queue_id: patchq-project-subpixel-projrepo-1
item_id: patchitem-branch-1
branch_id: branch-1
head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
visual_verdict: fail
severity: blocking`,
			})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "task_id"); got == validationTaskID {
				t.Fatalf("blocking visual evidence must not reuse stale validation task id %q", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if got := rpcString(req.Params, "description"); !strings.Contains(got, evidenceDocKey) || !strings.Contains(got, "visual_evidence_route") {
				t.Fatalf("revision description missing visual evidence context:\n%s", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-revision"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected revision follow-up from blocking visual evidence, got %+v", result)
	}
	for _, want := range []string{`"followup_kind": "revision"`, evidenceDocKey, `"task_submit_result"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"existing_task_id": "`+validationTaskID+`"`) {
		t.Fatalf("stale validation task must not be returned after blocking visual evidence receipt, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.doc.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolIgnoresGenericValidationBehindQueueBoundTask(t *testing.T) {
	headSHA := "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	item := ProjectPatchQueueItemRecord{
		QueueID:  "patchq-project-subpixel-projrepo-1",
		ItemID:   "patchitem-branch-1",
		BranchID: "branch-1",
		HeadSHA:  headSHA,
	}
	genericTaskID := "task-generic-visual-validation"
	validationTaskID := projectPatchQueueFollowupTaskID("project-subpixel", item, "validation")
	calls := make([]string, 0, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked only because browser/e2e and visual acceptance evidence were missing.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-05-25T12:00:00Z",
				"head_sha":         headSHA,
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{
				{
					"task_id":       genericTaskID,
					"title":         "Capture browser and visual acceptance evidence for Clearpress candidate 4dc89d3",
					"description":   "Generic visual sidecar for branch-1 and head " + headSHA + ".",
					"owner_user_id": "owner-1",
					"priority":      "high",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"project_id":    "project-subpixel",
					"project_lane":  "validation",
					"tags":          []string{"project", "patch-queue", "validation", "browser", "visual-acceptance"},
					"linked_by":     "agent-alpha",
					"updated_at":    "2026-05-14T12:01:00Z",
				},
				{
					"task_id":       validationTaskID,
					"title":         "Validate blocked integration candidate branch-1",
					"description":   "Patch queue validation follow-up.\n\n- queue_id: patchq-project-subpixel-projrepo-1\n- item_id: patchitem-branch-1\n- branch_id: branch-1\n- head_sha: " + headSHA,
					"owner_user_id": "owner-1",
					"priority":      "high",
					"status":        "RESOLVED",
					"task_kind":     "EXECUTION",
					"project_id":    "project-subpixel",
					"project_lane":  "validation",
					"tags":          []string{"project", "patch-queue", "validation", "blocked"},
					"linked_by":     "agent-alpha",
					"updated_at":    "2026-05-14T12:02:00Z",
				},
			}
			writeRPCResult(w, req, result)
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "task_id"); got == genericTaskID || got == validationTaskID {
				t.Fatalf("expected retry task instead of stale/exact terminal task id, got %q", got)
			}
			if got := rpcString(req.Params, "description"); !strings.Contains(got, "retry_of_terminal_followup_task: "+validationTaskID) {
				t.Fatalf("retry description missing terminal task receipt:\n%s", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-validation-retry"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected validation retry follow-up, got %+v", result)
	}
	for _, forbidden := range []string{`"existing_task_id": "` + genericTaskID + `"`, `"task_id": "` + genericTaskID + `"`} {
		if strings.Contains(result.Output, forbidden) {
			t.Fatalf("stale generic validation sidecar must not be returned, got %s", result.Output)
		}
	}
	for _, want := range []string{`"followup_kind": "validation"`, `"task_submit_result"`, "retry_of_terminal_followup_task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolDoesNotTreatBranchOnlyTaskWithoutHeadAsDuplicate(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked pending missing manual browser verification and production build evidence.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
				"head_sha":         "head123",
			})
			coordination := result["coordination"].(map[string]any)
			coordination["tasks"] = []map[string]any{
				{
					"task_id":       "task-random-branch-only-revision",
					"title":         "Unblock integration candidate branch-1",
					"description":   "Patch queue decision follow-up for branch_id: branch-1 without exact queue/item/head identity.",
					"owner_user_id": "owner-1",
					"priority":      "high",
					"status":        "PENDING",
					"task_kind":     "EXECUTION",
					"project_id":    "project-subpixel",
					"project_lane":  "implementation",
					"tags":          []string{"project", "patch-queue", "revision", "blocked"},
					"linked_by":     "agent-epsilon",
					"updated_at":    "2026-05-14T12:03:00Z",
				},
			}
			writeRPCResult(w, req, result)
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "validation" {
				t.Fatalf("project_lane = %q, want validation", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-validation"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"followup_kind": "validation"`) {
		t.Fatalf("expected fresh validation followup instead of stale branch-only duplicate, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.tasks.list,workspace.doc.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupTaskReferencesItemRejectsBranchPrefixCollision(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:  "patchq-project-subpixel-projrepo-1",
		ItemID:   "patchitem-branch-1",
		BranchID: "branch-1",
		HeadSHA:  "head123",
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-random-revision-branch-10",
		Title:       "Unblock integration candidate branch-10",
		Description: "Patch queue decision follow-up.\n\n- branch_id: branch-10\n- head_sha: head123\n- state: BLOCKED",
		ProjectLane: "implementation",
		Tags:        []string{"project", "patch-queue", "revision", "blocked"},
	}
	if projectPatchQueueFollowupTaskReferencesItem(task, item) {
		t.Fatalf("branch-10 task must not dedupe branch-1 item")
	}
}

func TestProjectPatchQueueFollowupTaskReferencesItemRejectsItemPrefixCollision(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:  "patchq-project-subpixel-projrepo-1",
		ItemID:   "patchitem-branch-1",
		BranchID: "branch-1",
		HeadSHA:  "head123",
	}
	task := WorkspaceTaskRecord{
		TaskID: "task-random-revision-branch-10-other",
		Title:  "Unblock integration candidate branch-10",
		Description: "Patch queue decision follow-up.\n\n" +
			"- queue_id: patchq-project-subpixel-projrepo-10\n" +
			"- item_id: patchitem-branch-10\n" +
			"- branch_id: branch-10\n" +
			"- head_sha: head123\n" +
			"- state: BLOCKED",
		ProjectLane: "implementation",
		Tags:        []string{"project", "patch-queue", "revision", "blocked"},
	}
	if projectPatchQueueFollowupTaskReferencesItem(task, item) {
		t.Fatalf("patchitem-branch-10 task must not dedupe patchitem-branch-1 item")
	}
}

func TestProjectPatchQueueFollowupToolKeepsNonDuplicateSubmitErrorActionable(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "ACCEPTED",
				"decision_summary": "Accepted and needs integration.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-alpha",
			}))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			writeRPCError(w, req, -32000, "project gate write denied")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected non-duplicate submit error to remain an error, got %+v", result)
	}
	if strings.Contains(result.Output, "Agents should claim the task") || strings.Contains(result.Output, "explicit follow-up work") {
		t.Fatalf("non-duplicate submit error must not tell agents to claim nonexistent follow-up task, got %s", result.Output)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolCreatesRebuildTaskForUnavailableAcceptedItem(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "ACCEPTED",
				"decision_summary": "Accepted candidate matched AC-01..AC-08, but source branch/head is now unavailable from current repository reality.",
				"decision_doc_key": "project.project-subpixel.patchq.decision",
				"decided_by":       "agent-kappa",
				"pathset":          []string{"src/**", "tests/**", "package.json"},
			}))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if got := rpcString(req.Params, "priority"); got != "high" {
				t.Fatalf("priority = %q, want high", got)
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "src/**,tests/**,package.json" {
				t.Fatalf("write_scope_hints = %q", got)
			}
			if got := rpcString(req.Params, "title"); !strings.Contains(got, "Rebuild unavailable accepted candidate") {
				t.Fatalf("title should describe rebuild, got %q", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{
				"source branch/head is unavailable",
				"Do not keep retrying the missing head_sha",
				"root/operator brief, acceptance criteria",
				"spec-fidelity failure",
				"project_branch_review_ready",
				"fresh patch queue item",
				"negative source-recovery evidence",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("description missing %q:\n%s", want, description)
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- project_lane: implementation") || !strings.Contains(content, "Rebuild unavailable accepted candidate") {
				t.Fatalf("canonical doc missing rebuild metadata:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-rebuild"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":    "project-subpixel",
		"branch_id":     "branch-1",
		"followup_kind": "rebuild",
		"extra_context": "Fresh cat-file checks proved the accepted head is absent.",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"followup_kind": "rebuild"`) {
		t.Fatalf("expected rebuild followup, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolRequiresNegativeEvidenceForRebuild(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
			"state":            "ACCEPTED",
			"decision_summary": "Accepted candidate.",
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":    "project-subpixel",
		"branch_id":     "branch-1",
		"followup_kind": "rebuild",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires concrete negative evidence") {
		t.Fatalf("expected negative evidence error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueFollowupToolRejectsRebuildForNonAcceptedItem(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
			"state":            "BLOCKED",
			"decision_summary": "Blocked pending browser evidence.",
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                "project-subpixel",
		"branch_id":                 "branch-1",
		"followup_kind":             "rebuild",
		"negative_evidence_doc_key": "task.some.negative_evidence",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires ACCEPTED") {
		t.Fatalf("expected non-accepted rebuild error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueFollowupTaskIDKeepsLongItemsDistinct(t *testing.T) {
	longPrefix := strings.Repeat("very-long-shared-prefix-", 8)
	left := ProjectPatchQueueItemRecord{
		QueueID: longPrefix + "queue-left",
		ItemID:  longPrefix + "item-left",
	}
	right := ProjectPatchQueueItemRecord{
		QueueID: longPrefix + "queue-right",
		ItemID:  longPrefix + "item-right",
	}
	leftID := projectPatchQueueFollowupTaskID("project-subpixel", left, "validation")
	rightID := projectPatchQueueFollowupTaskID("project-subpixel", right, "validation")
	if leftID == rightID {
		t.Fatalf("expected distinct task ids for long queue items, got %q", leftID)
	}
	if len(strings.TrimPrefix(leftID, "task-")) > 120 || len(strings.TrimPrefix(rightID, "task-")) > 120 {
		t.Fatalf("expected slug portion to stay bounded, got %d and %d", len(strings.TrimPrefix(leftID, "task-")), len(strings.TrimPrefix(rightID, "task-")))
	}
}

func TestProjectPatchQueueBlockedMixedCodeAndEvidenceCreatesRevision(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		State:           "BLOCKED",
		DecisionSummary: "Blocked after fresh review: build passes, but src/App.tsx contains corrupted user-facing UI strings and there is still no fresh browser/runtime smoke proving upload -> process -> preview -> export behavior.",
		DecisionDocKey:  "project.project-subpixel.patchq.blocked",
	}
	kind, err := normalizeProjectPatchQueueFollowupKind("auto", item)
	if err != nil {
		t.Fatalf("unexpected kind error: %v", err)
	}
	if kind != "revision" {
		t.Fatalf("mixed code defect and evidence gap should create revision follow-up, got %q", kind)
	}
	if !projectPatchQueueBlockedNeedsRevision(item) {
		t.Fatalf("expected code defect signals to require revision")
	}
	if projectPatchQueueBlockedNeedsValidation(item) {
		t.Fatalf("mixed code defect must not be treated as validation-only")
	}
}

func TestProjectPatchQueueBlockedSpecDriftTestOnlyCreatesRevision(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		State: "BLOCKED",
		DecisionSummary: strings.Join([]string{
			"BLOCKED_SPEC_DRIFT: fresh review of head 186def09f4dd88cf6375166e1fe26884db681a34 shows only internal/eval/eval_contract_test.go changes.",
			"The branch adds contract tests for parser/operator semantics but no runtime implementation change, so it does not deliver the claimed lane-scoped parser/operator semantics product increment on top of the green baseline.",
		}, " "),
		DecisionDocKey: "project.project-rq.patchq.parser-operators.blocked",
	}
	kind, err := normalizeProjectPatchQueueFollowupKind("auto", item)
	if err != nil {
		t.Fatalf("unexpected kind error: %v", err)
	}
	if kind != "revision" {
		t.Fatalf("spec drift with test-only implementation gap should create revision follow-up, got %q", kind)
	}
	if !projectPatchQueueBlockedNeedsRevision(item) {
		t.Fatalf("expected spec drift / no-runtime-implementation signals to require revision")
	}
	if projectPatchQueueBlockedNeedsValidation(item) {
		t.Fatalf("test-only implementation gap must not be treated as validation-only")
	}
}

func TestProjectPatchQueueBlockedVisualFailCreatesRevision(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		State: "BLOCKED",
		DecisionSummary: strings.Join([]string{
			"Visual acceptance packet says visual_verdict: fail.",
			"blocking_findings: board is below the first viewport on desktop and narrow, primary game surface is not visible on first load.",
			"responsive_fit: fail; result_state_coverage: fail.",
			"Smallest repair direction: rebalance layout and publish a repaired committed head.",
		}, " "),
		DecisionDocKey: "project.project-minesweeper.patchq.visual-fail",
	}
	kind, err := normalizeProjectPatchQueueFollowupKind("auto", item)
	if err != nil {
		t.Fatalf("unexpected kind error: %v", err)
	}
	if kind != "revision" {
		t.Fatalf("visual fail should create revision follow-up, got %q", kind)
	}
	if !projectPatchQueueBlockedNeedsRevision(item) {
		t.Fatalf("expected visual fail signals to require revision")
	}
	if projectPatchQueueBlockedNeedsValidation(item) {
		t.Fatalf("visual fail must not be treated as validation-only")
	}
}

func TestProjectPatchQueueFollowupToolCreatesRevisionTaskForBlockedItem(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked because exported SVG path and PNG path diverge.",
				"decision_doc_key": "project.project-subpixel.patchq.blocked-decision",
				"decided_by":       "agent-alpha",
				"pathset":          []string{},
				"pathset_json":     `{"paths":["web/export.js"]}`,
			}))
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("project_lane = %q, want implementation", got)
			}
			if got := rpcString(req.Params, "priority"); got != "high" {
				t.Fatalf("priority = %q, want high", got)
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "" {
				t.Fatalf("revision follow-up must not claim candidate pathset as write_scope_hints, got %q", got)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			if _, ok := requirements["write_scope_hints"]; ok {
				t.Fatalf("revision task_requirements must not claim candidate pathset as write_scope_hints: %+v", requirements)
			}
			if got := strings.Join(rpcStringSlice(requirements, "candidate_pathset"), ","); got != "web/export.js" {
				t.Fatalf("revision task_requirements candidate_pathset = %q, want web/export.js; requirements=%+v", got, requirements)
			}
			if got, _ := requirements["candidate_pathset_role"].(string); got != "historical_changed_path_evidence_not_claim_scope" {
				t.Fatalf("revision task_requirements candidate_pathset_role=%q; requirements=%+v", got, requirements)
			}
			for key, want := range map[string]string{
				"required_transition":             "project_patch_queue_revision_commit_review_submit",
				"required_first_publication_tool": "project_branch_commit",
				"required_terminal_tool":          "project_patch_queue_submit",
				"historical_source_branch_role":   "read_only_defeated_source_branch_evidence",
			} {
				if got, _ := requirements[key].(string); got != want {
					t.Fatalf("revision task_requirements %s=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			sequence := rpcStringSlice(requirements, "required_tool_sequence")
			if strings.Join(sequence, ",") != "project_branch_commit,project_branch_review_ready,project_patch_queue_submit" {
				t.Fatalf("revision task_requirements sequence=%v; requirements=%+v", sequence, requirements)
			}
			if !strings.Contains(rpcString(req.Params, "title"), "Unblock integration candidate") {
				t.Fatalf("title should ask to unblock candidate, got %q", rpcString(req.Params, "title"))
			}
			description := rpcString(req.Params, "description")
			if !strings.Contains(description, "candidate_pathset: web/export.js") || strings.Contains(description, "write_scope_hints: web/export.js") {
				t.Fatalf("revision follow-up should carry candidate pathset as evidence, not claim scope:\n%s", description)
			}
			for _, want := range []string{
				"queue_id: patchq-project-subpixel-projrepo-1",
				"item_id: patchitem-branch-1",
				"branch_id: branch-1",
				"review_doc_key: project.project-subpixel.branch.branch-1.review",
				"decision_doc_key: project.project-subpixel.patchq.blocked-decision",
				"decided_by: agent-alpha",
				"head_sha: head123",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("revision follow-up lineage missing %q:\n%s", want, description)
				}
			}
			if !strings.Contains(description, "project_branch_commit") || strings.Contains(description, "No git merge, push, rebase") {
				t.Fatalf("revision follow-up description should allow bounded revision publication without old blanket git ban, got %q", description)
			}
			for _, want := range []string{"not the referenced branch owner", "base_branch", "leave branch_name omitted", "fresh patch queue item_id"} {
				if !strings.Contains(description, want) {
					t.Fatalf("revision follow-up description missing %q:\n%s", want, description)
				}
			}
			tags := strings.Join(rpcStringSlice(req.Params, "tags"), ",")
			for _, want := range []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-1", "owner-agent:agent-alpha", "required-agent:agent-alpha"} {
				if !strings.Contains(tags, want) {
					t.Fatalf("revision follow-up tags missing %q: %s", want, tags)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
				"task_requirements_json": `{
					"schema":"task_requirements.v1",
					"required_work_modes":["implementation"],
					"source_doc_keys":["run.clearpress.operator-spec"],
					"spec_fidelity_contract":"source_artifact_fidelity.v1"
				}`,
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			for _, want := range []string{
				"queue_id: patchq-project-subpixel-projrepo-1",
				"item_id: patchitem-branch-1",
				"review_doc_key: project.project-subpixel.branch.branch-1.review",
				"decision_doc_key: project.project-subpixel.patchq.blocked-decision",
				"run.clearpress.operator-spec",
				"Source doc keys are non-droppable",
			} {
				if !strings.Contains(content, want) {
					t.Fatalf("canonical revision doc lost lineage %q:\n%s", want, content)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-revision"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":    "project-subpixel",
		"queue_id":      "patchq-project-subpixel-projrepo-1",
		"item_id":       "patchitem-branch-1",
		"extra_context": "Coordinate with the original branch owner before resubmitting.",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"followup_kind": "revision"`) {
		t.Fatalf("expected revision followup, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolKeepsBroadRevisionPathsetOutOfClaimScope(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked: candidate is data-only and lacks runnable browser surface.",
				"pathset":          []string{"src/**", "package.json", "package-lock.json", "vite.config.*", "tsconfig*.json", "index.html"},
			})
			coordination := result["coordination"].(map[string]any)
			coordination["branches"] = []map[string]any{{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"agent_id":         "agent-delta",
				"branch_name":      "agent-delta-data",
				"write_scope_json": `{"paths":["src/data/**","src/types/**","src/lib/**"]}`,
				"status":           "READY_FOR_REVIEW",
				"head_sha":         "head123",
				"review_doc_key":   "project.project-subpixel.branch.branch-1.review",
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "owner_user_id"); got != "agent-delta" {
				t.Fatalf("broad revision follow-up owner_user_id = %q, want branch owner agent-delta", got)
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "" {
				t.Fatalf("broad revision candidate pathset must not become claim write_scope_hints, got %q", got)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			if _, ok := requirements["write_scope_hints"]; ok {
				t.Fatalf("broad revision candidate pathset must not become task requirement write_scope_hints: %+v", requirements)
			}
			if got := strings.Join(rpcStringSlice(requirements, "candidate_pathset"), ","); got != "src/**,package.json,package-lock.json,vite.config.*,tsconfig*.json,index.html" {
				t.Fatalf("revision task_requirements candidate_pathset = %q; requirements=%+v", got, requirements)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{
				"candidate_pathset: src/**, package.json, package-lock.json, vite.config.*, tsconfig*.json, index.html",
				"not automatic claim scope",
				"branch_id: branch-1",
				"head_sha: head123",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("description missing %q:\n%s", want, description)
				}
			}
			tags := strings.Join(rpcStringSlice(req.Params, "tags"), ",")
			for _, want := range []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-1", "owner-agent:agent-delta", "required-agent:agent-delta"} {
				if !strings.Contains(tags, want) {
					t.Fatalf("revision follow-up tags missing %q: %s", want, tags)
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if strings.Contains(content, "- write_scope_hints: src/**") || !strings.Contains(content, "candidate_pathset: src/**") {
				t.Fatalf("canonical doc must not restate broad candidate pathset as write scope:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-revision"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-theta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":    "project-subpixel",
		"branch_id":     "branch-1",
		"followup_kind": "revision",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"followup_kind": "revision"`) {
		t.Fatalf("expected revision followup, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupTagsUseItemAgentFallbackWhenBranchProjectionMissing(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		State:    "BLOCKED",
		BranchID: "branch-1",
		AgentID:  "agent-delta",
	}
	owner := projectPatchQueueFollowupBranchOwnerAgentID(ProjectCoordinationRecord{}, item)
	if owner != "agent-delta" {
		t.Fatalf("owner fallback = %q, want agent-delta", owner)
	}
	tags := strings.Join(projectPatchQueueFollowupTags(item, "revision", owner), ",")
	for _, want := range []string{"owner-bound", "owner-bound-kind:patch_queue_revision", "owner-branch:branch-1", "owner-agent:agent-delta", "required-agent:agent-delta"} {
		if !strings.Contains(tags, want) {
			t.Fatalf("tags missing %q: %s", want, tags)
		}
	}
}

func TestProjectPatchQueueFollowupToolCreatesValidationTaskForBlockedEvidenceGap(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"decision_summary": "Blocked pending missing manual browser verification and production build evidence.",
				"pathset_json":     `{"paths":["web/app.js"]}`,
			}))
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "validation" {
				t.Fatalf("project_lane = %q, want validation", got)
			}
			if got := rpcString(req.Params, "title"); !strings.Contains(got, "Validate blocked integration candidate") {
				t.Fatalf("title should describe blocked validation, got %q", got)
			}
			if got := strings.Join(rpcStringSlice(req.Params, "write_scope_hints"), ","); got != "web/app.js" {
				t.Fatalf("write_scope_hints = %q", got)
			}
			requirements, ok := req.Params["task_requirements"].(map[string]any)
			if !ok {
				t.Fatalf("task_requirements missing: %+v", req.Params)
			}
			for key, want := range map[string]string{
				"patch_queue_task_identity": "rhizome_patch_queue_task_identity.v1",
				"patch_queue_task_kind":     "validation",
				"queue_id":                  "patchq-project-subpixel-projrepo-1",
				"item_id":                   "patchitem-branch-1",
				"branch_id":                 "branch-1",
				"head_sha":                  "head123",
				"required_transition":       "project_patch_queue_submit_or_revision_followup",
			} {
				if got, _ := requirements[key].(string); strings.TrimSpace(got) != want {
					t.Fatalf("task_requirements[%s]=%q want %q; requirements=%+v", key, got, want, requirements)
				}
			}
			if !strings.Contains(rpcString(req.Params, "description"), "Publish durable validation evidence") {
				t.Fatalf("description should ask for validation evidence, got %q", rpcString(req.Params, "description"))
			}
			for _, want := range []string{
				"terminal BLOCKED patch queue decision names missing evidence",
				"build/test/browser/smoke checks",
				"same branch_id and head_sha",
				"supersedes_item_id",
				"validation_doc_key/evidence_doc_key",
				"Completing with only a recommendation is incomplete",
				"Block only after a concrete bounded attempt",
			} {
				if !strings.Contains(rpcString(req.Params, "description"), want) {
					t.Fatalf("description missing %q: %q", want, rpcString(req.Params, "description"))
				}
			}
			writeRPCResult(w, req, map[string]any{"task_id": rpcString(req.Params, "task_id"), "workspace_id": "ws", "status": "PENDING"})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-validation"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-epsilon", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"followup_kind": "validation"`) {
		t.Fatalf("expected validation followup, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.list,workspace.tasks.list,workspace.doc.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueFollowupToolRejectsNonterminalItem(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{"state": "PROPOSED"}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "terminal decision state") {
		t.Fatalf("expected terminal-state error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueFollowupToolSkipsCanceledWithoutTaskSubmit(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
			"state":            "CANCELED",
			"decision_summary": "Canceled because candidate was superseded.",
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"created": false`) || !strings.Contains(result.Output, "canceled_patch_queue_item") {
		t.Fatalf("expected canceled no-task result, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueFollowupToolRejectsAmbiguousBranchSelector(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		first := patchQueueLifecycleItem(map[string]any{
			"state":            "ACCEPTED",
			"decision_summary": "First accepted decision.",
		})
		second := patchQueueLifecycleItem(map[string]any{
			"queue_id":         "patchq-project-subpixel-projrepo-2",
			"item_id":          "patchitem-branch-1-second",
			"state":            "ACCEPTED",
			"decision_summary": "Second accepted decision.",
		})
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{first, second}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueFollowupTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "ambiguous") {
		t.Fatalf("expected ambiguous selector error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}
