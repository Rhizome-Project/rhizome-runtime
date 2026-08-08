package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectPatchQueueControlledAddFileCrossAgentFlow(t *testing.T) {
	repoRoot, baseSHA, headSHA, headDigest := setupPatchQueueGitRepoWithAddedFile(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")

	const (
		workerID     = "worker-alpha"
		integratorID = "integrator-beta"
		projectID    = "project-subpixel"
		queueID      = "patchq-project-subpixel-projrepo-1"
		itemID       = "patchitem-branch-1"
		claimToken   = "claim-token-integrator-beta"
	)

	var item map[string]any
	calls := make([]string, 0, 9)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, controlledAddFileFlowCoordinationResult(repoRoot, baseSHA, headSHA, item))
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "actor_id"); got != workerID {
				t.Fatalf("submit actor_id = %q, want %q", got, workerID)
			}
			if got := rpcString(req.Params, "repo_authority_mode"); got != "repoauthority_controlled_queue" {
				t.Fatalf("submit repo_authority_mode = %q", got)
			}
			if got := rpcString(req.Params, "pathset_json"); got != "" && got != `{"paths":["web/**"]}` {
				t.Fatalf("submit pathset_json = %q", got)
			}
			if got := rpcString(req.Params, "base_tree_hash"); got != baseTreeHash {
				t.Fatalf("submit base_tree_hash = %q, want %q", got, baseTreeHash)
			}
			if got := rpcString(req.Params, "repo_root"); !strings.EqualFold(got, repoRoot) {
				t.Fatalf("submit repo_root = %q, want %q", got, repoRoot)
			}
			baseHashes, _ := req.Params["base_file_hashes"].(map[string]any)
			if len(baseHashes) != 0 {
				t.Fatalf("added-only submit must not synthesize base_file_hashes, got %+v", baseHashes)
			}
			item = controlledAddFileFlowItem(repoRoot, baseSHA, headSHA, baseTreeHash, map[string]any{
				"state": "PROPOSED",
			})
			writeRPCResult(w, req, map[string]any{"patch_queue_item": item})
		case "project.patch_queue.claim":
			if got := rpcString(req.Params, "actor_id"); got != integratorID {
				t.Fatalf("claim actor_id = %q, want %q", got, integratorID)
			}
			item = controlledAddFileFlowItem(repoRoot, baseSHA, headSHA, baseTreeHash, map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       integratorID,
				"claim_token":      claimToken,
				"claim_expires_at": "2026-04-28T12:00:00Z",
			})
			writeRPCResult(w, req, map[string]any{"patch_queue_item": item})
		case "project.patch_queue.operation_bind":
			if got := rpcString(req.Params, "actor_id"); got != integratorID {
				t.Fatalf("operation_bind actor_id = %q, want %q", got, integratorID)
			}
			if got := rpcString(req.Params, "claim_token"); got != claimToken {
				t.Fatalf("operation_bind claim_token = %q", got)
			}
			if got := rpcString(req.Params, "mutation_paths_json"); got != `{"paths":["web/**"]}` {
				t.Fatalf("operation_bind mutation_paths_json = %q", got)
			}
			item = controlledAddFileFlowItem(repoRoot, baseSHA, headSHA, baseTreeHash, map[string]any{
				"state":                          "CLAIMED",
				"claimed_by":                     integratorID,
				"claim_token":                    claimToken,
				"claim_expires_at":               "2026-04-28T12:00:00Z",
				"operation_id":                   "op-add-flow-1",
				"operation_kind":                 projectPatchQueueOperationKindPatchApply,
				"operation_binding_schema":       "project_patch_queue_operation_binding.v1",
				"operation_binding_accepted":     true,
				"operation_context_digest":       "sha256:operation-context",
				"operation_lease_context_digest": "sha256:operation-lease-context",
				"operation_mutation_paths_json":  `{"paths":["web/**"]}`,
				"operation_bound_by":             integratorID,
				"operation_bound_at":             "2026-04-28T12:01:00Z",
			})
			writeRPCResult(w, req, map[string]any{"patch_queue_item": item})
		case "project.patch_queue.cas_record":
			if got := rpcString(req.Params, "actor_id"); got != integratorID {
				t.Fatalf("cas_record actor_id = %q, want %q", got, integratorID)
			}
			if got := rpcString(req.Params, "claim_token"); got != claimToken {
				t.Fatalf("cas_record claim_token = %q", got)
			}
			casResult, ok := req.Params["cas_result"].(map[string]any)
			if !ok {
				t.Fatalf("cas_result missing: %+v", req.Params)
			}
			if casResult["schema"] != projectPatchQueueCASApplySchema ||
				casResult["status"] != projectPatchQueueCASStatusApplied ||
				casResult["patch_id"] != "op-add-flow-1" ||
				casResult["context_digest"] != "sha256:operation-context" {
				t.Fatalf("unexpected add-file CAS metadata: %+v", casResult)
			}
			paths, ok := casResult["paths"].([]any)
			if !ok || len(paths) != 1 {
				t.Fatalf("expected one add-file CAS path, got %+v", casResult["paths"])
			}
			path, ok := paths[0].(map[string]any)
			if !ok {
				t.Fatalf("unexpected add-file CAS path payload: %+v", paths[0])
			}
			if path["path"] != "web/new.js" ||
				path["status"] != projectPatchQueueCASStatusApplied ||
				path["change_kind"] != projectPatchQueueCASChangeAdd ||
				path["candidate_hash"] != headDigest ||
				strings.TrimSpace(rpcString(path, "base_hash")) != "" ||
				strings.TrimSpace(rpcString(path, "current_hash")) != "" {
				t.Fatalf("unexpected add-file CAS path evidence: %+v", path)
			}
			item = controlledAddFileFlowItem(repoRoot, baseSHA, headSHA, baseTreeHash, map[string]any{
				"state":                          "CLAIMED",
				"claimed_by":                     integratorID,
				"claim_token":                    claimToken,
				"claim_expires_at":               "2026-04-28T12:00:00Z",
				"operation_id":                   "op-add-flow-1",
				"operation_kind":                 projectPatchQueueOperationKindPatchApply,
				"operation_binding_schema":       "project_patch_queue_operation_binding.v1",
				"operation_binding_accepted":     true,
				"operation_context_digest":       "sha256:operation-context",
				"operation_lease_context_digest": "sha256:operation-lease-context",
				"operation_mutation_paths_json":  `{"paths":["web/**"]}`,
				"operation_bound_by":             integratorID,
				"operation_bound_at":             "2026-04-28T12:01:00Z",
				"cas_evidence_accepted":          true,
				"cas_status":                     projectPatchQueueCASStatusApplied,
				"cas_patch_digest":               casResult["patch_digest"],
				"cas_evaluation_digest":          "sha256:cas-evaluation",
				"cas_result":                     casResult,
				"cas_recorded_by":                integratorID,
			})
			writeRPCResult(w, req, map[string]any{"patch_queue_item": item})
		case "project.patch_queue.materialization_record":
			if got := rpcString(req.Params, "actor_id"); got != integratorID {
				t.Fatalf("materialization actor_id = %q, want %q", got, integratorID)
			}
			if got := rpcString(req.Params, "claim_token"); got != claimToken {
				t.Fatalf("materialization claim_token = %q", got)
			}
			materialization, ok := req.Params["materialization"].(map[string]any)
			if !ok {
				t.Fatalf("expected materialization object, got %+v", req.Params["materialization"])
			}
			files, ok := materialization["files"].([]any)
			if !ok || len(files) != 1 {
				t.Fatalf("expected one materialized file, got %+v", materialization["files"])
			}
			file, ok := files[0].(map[string]any)
			if !ok {
				t.Fatalf("expected file object, got %+v", files[0])
			}
			if file["path"] != "web/new.js" ||
				file["change_kind"] != projectPatchQueueCASChangeAdd ||
				file["candidate_hash"] != headDigest ||
				file["content_digest"] != headDigest ||
				file["content"] != "new file\n" ||
				strings.TrimSpace(rpcString(file, "base_hash")) != "" {
				t.Fatalf("unexpected add-file materialized file: %+v", file)
			}
			item["materialization_accepted"] = true
			item["materialization_digest"] = "sha256:materialization"
			item["materialization_recorded_by"] = integratorID
			writeRPCResult(w, req, map[string]any{"patch_queue_item": item})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	submit := NewProjectPatchQueueSubmitTool(client, "ws", workerID).
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-add-flow",
				SessionID:                "session-add-flow",
				RunID:                    "run-add-flow",
				CapabilitySnapshotID:     "snapshot-add-flow",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	submitted := submit.Execute(context.Background(), map[string]any{
		"project_id":       projectID,
		"branch_id":        "branch-1",
		"controlled_queue": true,
		"repo_lease_id":    "lease-add-flow",
		"lease_term":       2,
	})
	if submitted == nil || submitted.IsError || !strings.Contains(submitted.Output, `"no_git_mutation": true`) {
		t.Fatalf("expected worker submit success, got %+v", submitted)
	}

	lifecycle := NewProjectPatchQueueLifecycleTool(client, "ws", integratorID)
	claimed := lifecycle.Execute(context.Background(), map[string]any{
		"project_id": projectID,
		"action":     "claim",
		"branch_id":  "branch-1",
	})
	if claimed == nil || claimed.IsError || !strings.Contains(claimed.Output, `"claimed_by": "`+integratorID+`"`) {
		t.Fatalf("expected integrator claim success, got %+v", claimed)
	}

	cas := NewProjectPatchQueueCASTool(client, "ws", integratorID, repoRoot)
	casRecorded := cas.Execute(context.Background(), map[string]any{
		"project_id":          projectID,
		"branch_id":           "branch-1",
		"test_name":           "controlled add-file flow",
		"test_command":        "go test ./...",
		"test_status":         projectPatchQueueTestStatusPassed,
		"test_exit_code":      0,
		"test_output_summary": "ok",
	})
	if casRecorded == nil || casRecorded.IsError ||
		!strings.Contains(casRecorded.Output, headDigest) ||
		!strings.Contains(casRecorded.Output, `"no_git_mutation": true`) ||
		strings.Contains(casRecorded.Output, "new file") {
		t.Fatalf("expected integrator CAS success without raw content, got %+v", casRecorded)
	}

	materialize := NewProjectPatchQueueMaterializeTool(client, "ws", integratorID, repoRoot)
	materialized := materialize.Execute(context.Background(), map[string]any{
		"project_id": projectID,
		"branch_id":  "branch-1",
	})
	if materialized == nil || materialized.IsError ||
		!strings.Contains(materialized.Output, `"materialization_digest": "sha256:materialization"`) ||
		!strings.Contains(materialized.Output, headDigest) ||
		!strings.Contains(materialized.Output, `"no_git_mutation": true`) ||
		strings.Contains(materialized.Output, "new file") {
		t.Fatalf("expected integrator materialization success without raw content, got %+v", materialized)
	}

	wantCalls := []string{
		"project.coordination.get",
		"project.patch_queue.submit",
		"project.coordination.get",
		"project.patch_queue.claim",
		"project.coordination.get",
		"project.patch_queue.operation_bind",
		"project.patch_queue.cas_record",
		"project.coordination.get",
		"project.patch_queue.materialization_record",
	}
	if strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") {
		t.Fatalf("unexpected RPC calls:\n got: %v\nwant: %v", calls, wantCalls)
	}
}

func controlledAddFileFlowCoordinationResult(checkoutPath, baseSHA, headSHA string, item map[string]any) map[string]any {
	result := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "worker-alpha",
		BranchStatus:   "READY_FOR_REVIEW",
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	})
	coordination := result["coordination"].(map[string]any)
	branches := coordination["branches"].([]map[string]any)
	branches[0]["review_doc_key"] = "project.project-subpixel.branch.branch-1.review"
	branches[0]["base_sha"] = baseSHA
	branches[0]["head_sha"] = headSHA
	coordination["branches"] = branches
	coordination["roles"] = []map[string]any{{
		"role_id":      "role-integrator-beta",
		"workspace_id": "ws",
		"project_id":   "project-subpixel",
		"agent_id":     "integrator-beta",
		"role_type":    "INTEGRATOR",
		"status":       "ACTIVE",
	}}
	if item != nil {
		coordination["patch_queue_items"] = []map[string]any{item}
	}
	return result
}

func controlledAddFileFlowItem(repoRoot, baseSHA, headSHA, baseTreeHash string, overrides map[string]any) map[string]any {
	item := patchQueueLifecycleItem(map[string]any{
		"queue_id":                   "patchq-project-subpixel-projrepo-1",
		"item_id":                    "patchitem-branch-1",
		"repo_authority_mode":        "repoauthority_controlled_queue",
		"pathset":                    []string{"web/**"},
		"pathset_json":               `{"paths":["web/**"]}`,
		"base_sha":                   baseSHA,
		"head_sha":                   headSHA,
		"base_file_hashes":           map[string]string{},
		"repo_root":                  repoRoot,
		"base_tree_hash":             baseTreeHash,
		"context_digest":             "sha256:proposal-context",
		"repo_lease_id":              "lease-add-flow",
		"lease_term":                 2,
		"task_id":                    "task-add-flow",
		"session_id":                 "session-add-flow",
		"run_id":                     "run-add-flow",
		"agent_id":                   "worker-alpha",
		"principal_type":             "agent",
		"principal_id":               "worker-alpha",
		"submitted_by":               "worker-alpha",
		"capability_snapshot_id":     "snapshot-add-flow",
		"capability_snapshot_schema": daemonCapabilitySnapshotSchema,
	})
	for key, value := range overrides {
		item[key] = value
	}
	return item
}
