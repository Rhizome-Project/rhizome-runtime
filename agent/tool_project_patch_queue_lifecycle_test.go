package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectPatchQueueAuthoritySplitsReviewEvidenceFromCanonicalIntegration(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Roles: []ProjectRoleRecord{{
			AgentID:  "reviewer-alpha",
			RoleType: "REVIEWER",
			Status:   "ACTIVE",
		}},
	}
	if !projectAgentHasPatchQueueEvidenceAuthority(coordination, "reviewer-alpha") {
		t.Fatal("reviewer should be allowed to record patch queue evidence")
	}
	if projectAgentHasPatchQueueIntegrationAuthority(coordination, "reviewer-alpha") {
		t.Fatal("reviewer must not gain canonical integration authority")
	}
	coordination.Roles[0].RoleType = "INTEGRATOR"
	if !projectAgentHasPatchQueueEvidenceAuthority(coordination, "reviewer-alpha") ||
		!projectAgentHasPatchQueueIntegrationAuthority(coordination, "reviewer-alpha") {
		t.Fatal("integrator should have both evidence and integration authority")
	}
}

func TestProjectPatchQueueLifecycleToolClaimsAndAccepts(t *testing.T) {
	calls := make([]string, 0, 8)
	visualPacket := completeStructuredVisualPacketWithRealScreenshots(t)
	visualPacket = strings.ReplaceAll(visualPacket, "branch-theta", "branch-1")
	visualPacket = strings.ReplaceAll(visualPacket, "b7f175bf9b8d027163f730e244f2ce2c8f186313", "head123")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			item := map[string]any{"state": "PROPOSED"}
			if len(calls) > 4 {
				item = map[string]any{
					"state":            "ACCEPTED",
					"claimed_by":       "agent-alpha",
					"claim_token":      "claim-token-1",
					"decision_summary": "Accepted after integration review.",
					"decided_by":       "agent-alpha",
					"decided_at":       "2026-04-28T12:01:00Z",
				}
			} else if len(calls) > 2 {
				item = map[string]any{
					"state":       "CLAIMED",
					"claimed_by":  "agent-alpha",
					"claim_token": "claim-token-1",
				}
			}
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(item))
		case "project.patch_queue.claim":
			if got := rpcString(req.Params, "actor_id"); got != "agent-alpha" {
				t.Fatalf("unexpected actor_id %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       "agent-alpha",
				"claim_token":      "claim-token-1",
				"claim_expires_at": "2026-04-28T12:00:00Z",
			})})
		case "project.patch_queue.decision":
			if got := rpcString(req.Params, "claim_token"); got != "claim-token-1" {
				t.Fatalf("unexpected claim_token %q", got)
			}
			if got := rpcString(req.Params, "decision"); got != "ACCEPTED" {
				t.Fatalf("unexpected decision %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "ACCEPTED",
				"claimed_by":       "agent-alpha",
				"claim_token":      "claim-token-1",
				"decision_summary": "Accepted after integration review.",
				"decision_doc_key": "project.project-subpixel.visual.acceptance",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-04-28T12:01:00Z",
			})})
		case "workspace.doc.get":
			switch rpcString(req.Params, "doc_key") {
			case "project.project-subpixel.visual.acceptance":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.visual.acceptance",
					"title":   "Visual Acceptance - Subpixel",
					"content": visualPacket,
				})
			case "project.project-subpixel.branch.branch-1.review":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.branch.branch-1.review",
					"title":   "Branch Review Packet",
					"content": "browser smoke and review packet for frontend branch",
				})
			default:
				t.Fatalf("unexpected workspace.doc.get key %q", rpcString(req.Params, "doc_key"))
			}
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "integration" {
				t.Fatalf("expected accepted decision to create integration follow-up, got lane %q", got)
			}
			if got := rpcString(req.Params, "task_kind"); got != "EXECUTION" {
				t.Fatalf("unexpected follow-up task_kind %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- project_lane: integration") || !strings.Contains(content, "project_patch_queue_integrate") {
				t.Fatalf("expected integration canonical doc, got:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	claim := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "claim",
		"branch_id":  "branch-1",
	})
	if claim == nil || claim.IsError || !strings.Contains(claim.Output, `"state": "CLAIMED"`) {
		t.Fatalf("expected claim success, got %+v", claim)
	}
	decision := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Accepted after integration review.",
		"decision_doc_key": "project.project-subpixel.visual.acceptance",
	})
	if decision == nil || decision.IsError || !strings.Contains(decision.Output, `"state": "ACCEPTED"`) || !strings.Contains(decision.Output, `"integration_followup"`) {
		t.Fatalf("expected decision success, got %+v", decision)
	}
	for _, want := range []string{`"next_action_contract"`, `"contract_version": "patch_queue_lifecycle_next_action_v1"`, `"current_work_transition": "complete_or_release_current_patch_queue_lifecycle_task"`, `"followup_visibility": "visible_task_created_or_hydrated"`} {
		if !strings.Contains(decision.Output, want) {
			t.Fatalf("expected next-action contract to contain %q, got %s", want, decision.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.claim,project.coordination.get,workspace.doc.get,workspace.doc.get,project.patch_queue.decision,project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolPassesCheckedSourceDocKeys(t *testing.T) {
	var decisionChecked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
				"pathset":     []string{"internal/lexer/lexer.go"},
			}))
		case "workspace.doc.get":
			switch rpcString(req.Params, "doc_key") {
			case "project.project-subpixel.source_refs":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.source_refs",
					"title":   "Source Refs",
					"content": "```rhizome_source_refs_v1\nsource_doc_keys:\n- operator.signal01-run25.spec\n```",
				})
			case "project.project-subpixel.source_requirements_trace":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.source_requirements_trace",
					"title":   "Source Requirements Trace",
					"content": "```rhizome_source_requirements_trace_v1\nsource_doc_keys:\n- operator.signal01-run25.spec\nacceptance_critical_anchors:\n- lexer edge cases\nacceptance_criteria_refs:\n- AC-lexer\nadjacent_wrong_products:\n- unrelated parser-only branch\n```",
				})
			case "project.project-subpixel.patchq.branch-1.decision":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.patchq.branch-1.decision",
					"title":   "Patch Queue Decision",
					"content": "```rhizome_spec_fidelity_review_v1\nsource_fidelity_status: passed\nnotes: lane review checked the lexer anchors, but the checked_source_doc_keys field is omitted here.\n```",
				})
			case "project.project-subpixel.branch.branch-1.review":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.branch.branch-1.review",
					"title":   "Branch Review Packet",
					"content": "review packet for internal lexer lane",
				})
			default:
				t.Fatalf("unexpected workspace.doc.get key %q", rpcString(req.Params, "doc_key"))
			}
		case "project.patch_queue.decision":
			if got := rpcString(req.Params, "decision"); got != "ACCEPTED" {
				t.Fatalf("unexpected decision %q", got)
			}
			decisionChecked = rpcStringSlice(req.Params, "checked_source_doc_keys")
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "ACCEPTED",
				"claimed_by":       "agent-alpha",
				"claim_token":      "claim-token-1",
				"decision_summary": "Accepted with source_fidelity_status: passed.",
				"decision_doc_key": "project.project-subpixel.patchq.branch-1.decision",
				"decided_by":       "agent-alpha",
				"decided_at":       "2026-06-01T12:01:00Z",
			})})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Accepted with source_fidelity_status: passed.",
		"decision_doc_key": "project.project-subpixel.patchq.branch-1.decision",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected decision success, got %+v", result)
	}
	if len(decisionChecked) != 1 || decisionChecked[0] != "operator.signal01-run25.spec" {
		t.Fatalf("checked_source_doc_keys = %#v", decisionChecked)
	}
}

func TestProjectPatchQueueLifecycleToolBlocksUIAcceptWithoutVisualPacket(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
				"pathset":     []string{"index.html", "src/App.tsx", "src/styles.css"},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key": rpcString(req.Params, "doc_key"),
				"title":   "Weak review",
				"content": "npm test passed and preview smoke found the app marker; no screenshot evidence.",
			})
		case "project.patch_queue.decision":
			t.Fatalf("accept decision should not reach RPC without visual acceptance")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Accepting from green build and browser smoke.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected visual acceptance gate error, got %+v", result)
	}
	for _, want := range []string{"visual_acceptance_gate", "rhizome_visual_acceptance_v1", "screenshot", "viewport", "overlap", "action=block"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolBlocksUIAcceptWithMissingScreenshotArtifacts(t *testing.T) {
	calls := make([]string, 0, 4)
	packet := strings.ReplaceAll(completeStructuredVisualPacket(), "branch-theta", "branch-1")
	packet = strings.ReplaceAll(packet, "b7f175bf9b8d027163f730e244f2ce2c8f186313", "head123")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
				"pathset":     []string{"src/App.tsx", "src/styles.css"},
			}))
		case "workspace.doc.get":
			switch rpcString(req.Params, "doc_key") {
			case "project.project-subpixel.visual.acceptance":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.visual.acceptance",
					"title":   "Visual Acceptance",
					"content": packet,
				})
			case "project.project-subpixel.branch.branch-1.review":
				writeRPCResult(w, req, map[string]any{
					"doc_key": rpcString(req.Params, "doc_key"),
					"title":   "Branch Review",
					"content": "frontend review packet",
				})
			default:
				t.Fatalf("unexpected workspace.doc.get key %q", rpcString(req.Params, "doc_key"))
			}
		case "project.patch_queue.decision":
			t.Fatalf("accept decision should not reach RPC with missing screenshot artifacts")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Accepting with visual evidence.",
		"decision_doc_key": "project.project-subpixel.visual.acceptance",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing screenshot artifact gate error, got %+v", result)
	}
	for _, want := range []string{"visual_acceptance_gate", "local screenshot artifact missing"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.get,workspace.doc.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRejectsInlineOnlyVisualAcceptancePacket(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
				"pathset":     []string{"src/**", "package.json"},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, map[string]any{
				"doc_key": rpcString(req.Params, "doc_key"),
				"title":   "Weak review",
				"content": "browser smoke loaded the React app but no durable visual acceptance packet exists.",
			})
		case "project.patch_queue.decision":
			t.Fatalf("inline-only visual packet should not reach decision RPC")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	inlinePacket := strings.ReplaceAll(completeStructuredVisualPacket(), "branch-theta", "branch-1")
	inlinePacket = strings.ReplaceAll(inlinePacket, "b7f175bf9b8d027163f730e244f2ce2c8f186313", "head123")
	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": inlinePacket,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "visual_acceptance_gate") {
		t.Fatalf("expected inline-only visual acceptance to be rejected, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolBlocksUIAcceptWithMismatchedVisualPacketHead(t *testing.T) {
	calls := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
				"pathset":     []string{"src/**", "package.json"},
			}))
		case "workspace.doc.get":
			switch rpcString(req.Params, "doc_key") {
			case "project.project-subpixel.visual.foreign":
				writeRPCResult(w, req, map[string]any{
					"doc_key": "project.project-subpixel.visual.foreign",
					"title":   "Foreign visual packet",
					"content": completeStructuredVisualPacket(),
				})
			case "project.project-subpixel.branch.branch-1.review":
				writeRPCResult(w, req, map[string]any{
					"doc_key": rpcString(req.Params, "doc_key"),
					"title":   "Branch Review",
					"content": "frontend review packet",
				})
			default:
				t.Fatalf("unexpected workspace.doc.get key %q", rpcString(req.Params, "doc_key"))
			}
		case "project.patch_queue.decision":
			t.Fatalf("mismatched visual packet should not reach decision RPC")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Accepting with visual evidence.",
		"decision_doc_key": "project.project-subpixel.visual.foreign",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected mismatched visual packet to be rejected, got %+v", result)
	}
	for _, want := range []string{"visual packet candidate provenance", "visual packet head_sha"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to mention %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,workspace.doc.get,workspace.doc.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRejectsBlockedActorAuthorityGap(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-iota",
				"claim_token": "claim-token-1",
				"branch_id":   "branch-evaluator",
				"head_sha":    "head123",
				"pathset":     []string{"internal/rq/eval.go"},
			}))
		case "project.patch_queue.decision":
			t.Fatalf("authority-gap BLOCKED decision should not reach RPC")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-iota")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "block",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Fresh review passed for this lane, but controlled-queue completion is blocked because iota lacks INTEGRATOR authority.",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected authority-gap BLOCKED decision to be refused, got %+v", result)
	}
	for _, want := range []string{"reviewer/integrator authority gap", "BLOCKED is for candidate defects"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolAutoCreatesValidationFollowupForBlockedEvidenceGap(t *testing.T) {
	calls := make([]string, 0, 6)
	coordinationReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			coordinationReads++
			item := map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       "agent-zeta",
				"claim_token":      "claim-token-1",
				"pathset_json":     `{"paths":["src/**","package.json"]}`,
				"decision_summary": "Missing browser/runtime evidence.",
			}
			if coordinationReads > 1 {
				item = map[string]any{
					"state":            "BLOCKED",
					"claimed_by":       "agent-zeta",
					"claim_token":      "claim-token-1",
					"decision_summary": "Missing browser/runtime evidence for upload, preview, export, and channel mapping.",
					"decision_doc_key": "project.project-subpixel.patchq.blocked",
					"decided_by":       "agent-zeta",
					"decided_at":       "2026-04-28T12:01:00Z",
					"pathset_json":     `{"paths":["src/**","package.json"]}`,
				}
			}
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(item))
		case "project.patch_queue.decision":
			if got := rpcString(req.Params, "decision"); got != "BLOCKED" {
				t.Fatalf("unexpected decision %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "BLOCKED",
				"claimed_by":       "agent-zeta",
				"claim_token":      "claim-token-1",
				"decision_summary": "Missing browser/runtime evidence for upload, preview, export, and channel mapping.",
				"decision_doc_key": "project.project-subpixel.patchq.blocked",
				"decided_by":       "agent-zeta",
				"decided_at":       "2026-04-28T12:01:00Z",
				"pathset_json":     `{"paths":["src/**","package.json"]}`,
			})})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "validation" {
				t.Fatalf("expected blocked evidence gap to create validation follow-up, got lane %q", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{
				"terminal BLOCKED patch queue decision names missing evidence",
				"build/test/browser/smoke checks",
				"same branch_id and head_sha",
				"supersedes_item_id, supersedes_queue_id, and validation_doc_key/evidence_doc_key",
			} {
				if !strings.Contains(description, want) {
					t.Fatalf("description missing %q:\n%s", want, description)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- project_lane: validation") {
				t.Fatalf("expected validation canonical doc, got:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-zeta")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "block",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Missing browser/runtime evidence for upload, preview, export, and channel mapping.",
		"decision_doc_key": "project.project-subpixel.patchq.blocked",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"state": "BLOCKED"`) || !strings.Contains(result.Output, `"followup"`) || !strings.Contains(result.Output, `"followup_kind": "validation"`) {
		t.Fatalf("expected blocked decision with validation follow-up, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.decision,project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolAutoCreatesRevisionFollowupForVisualBlocker(t *testing.T) {
	calls := make([]string, 0, 6)
	coordinationReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			coordinationReads++
			item := map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-zeta",
				"claim_token": "claim-token-1",
				"branch_id":   "branch-1",
				"head_sha":    "b7f175bf9b8d027163f730e244f2ce2c8f186313",
				"pathset":     []string{"src/**", "package.json"},
			}
			if coordinationReads > 1 {
				item = map[string]any{
					"state":            "BLOCKED",
					"claimed_by":       "agent-zeta",
					"claim_token":      "claim-token-1",
					"branch_id":        "branch-1",
					"head_sha":         "b7f175bf9b8d027163f730e244f2ce2c8f186313",
					"decision_summary": "Visual QA found a broken first viewport, giant heading, and excessive whitespace; this is a user-facing UI defect requiring repair.",
					"decision_doc_key": "project.project-subpixel.patchq.visual-blocked",
					"decided_by":       "agent-zeta",
					"decided_at":       "2026-04-28T12:01:00Z",
					"pathset":          []string{"src/**", "package.json"},
				}
			}
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(item))
		case "project.patch_queue.decision":
			if got := rpcString(req.Params, "decision"); got != "BLOCKED" {
				t.Fatalf("unexpected decision %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "BLOCKED",
				"claimed_by":       "agent-zeta",
				"claim_token":      "claim-token-1",
				"branch_id":        "branch-1",
				"head_sha":         "b7f175bf9b8d027163f730e244f2ce2c8f186313",
				"decision_summary": "Visual QA found a broken first viewport, giant heading, and excessive whitespace; this is a user-facing UI defect requiring repair.",
				"decision_doc_key": "project.project-subpixel.patchq.visual-blocked",
				"decided_by":       "agent-zeta",
				"decided_at":       "2026-04-28T12:01:00Z",
				"pathset":          []string{"src/**", "package.json"},
			})})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{}})
		case "task.submit":
			if got := rpcString(req.Params, "project_lane"); got != "implementation" {
				t.Fatalf("expected visual blocker to create revision implementation follow-up, got lane %q", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"Revise or unblock the candidate", "broken first viewport", "branch_id: branch-1"} {
				if !strings.Contains(description, want) {
					t.Fatalf("description missing %q:\n%s", want, description)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			content := rpcString(req.Params, "content")
			if !strings.Contains(content, "- project_lane: implementation") {
				t.Fatalf("expected implementation canonical doc, got:\n%s", content)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-followup"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-zeta")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "block",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"claim_token":      "claim-token-1",
		"decision_summary": "Visual QA found a broken first viewport, giant heading, and excessive whitespace; this is a user-facing UI defect requiring repair.",
		"decision_doc_key": "project.project-subpixel.patchq.visual-blocked",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"state": "BLOCKED"`) || !strings.Contains(result.Output, `"followup_kind": "revision"`) {
		t.Fatalf("expected blocked visual decision with revision follow-up, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.decision,project.coordination.get,workspace.tasks.list,task.submit,workspace.doc.put" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolDefersAuthorityToServer(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueLifecycleCoordinationResult(map[string]any{
				"state": "PROPOSED",
			})
			coordination := result["coordination"].(map[string]any)
			coordination["roles"] = []map[string]any{}
			coordination["strategic_lead"] = nil
			writeRPCResult(w, req, result)
		case "project.patch_queue.claim":
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       "agent-alpha",
				"claim_token":      "claim-token-1",
				"claim_expires_at": "2026-04-28T12:00:00Z",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "claim",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"state": "CLAIMED"`) {
		t.Fatalf("expected server-authorized claim, got %+v", result)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.claim" {
		t.Fatalf("expected coordination then server claim call, got %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolReconcilesReviewTaskReceipt(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state": "PROPOSED",
			}))
		case "project.patch_queue.review_task.reconcile":
			for key, want := range map[string]string{
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"actor_id":     "agent-alpha",
				"queue_id":     "patchq-project-subpixel-projrepo-1",
				"item_id":      "patchitem-branch-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"patch_queue_review_task": map[string]any{
					"task_id":      "task-patchq-review-project-subpixel-patchitem-branch-1",
					"workspace_id": "ws",
					"status":       "PENDING",
					"priority":     "high",
					"project_id":   "project-subpixel",
					"project_lane": "review",
					"created_at":   "2026-05-26T00:00:00Z",
					"updated_at":   "2026-05-26T00:00:00Z",
				},
				"review_task_event_id": "event-review-task-repaired",
				"repaired":             true,
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "reconcile_review_task",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected reconcile review task success, got %+v", result)
	}
	for _, want := range []string{`"action": "reconcile_review_task"`, `"repaired": true`, "event-review-task-repaired", "task-patchq-review-project-subpixel"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.review_task.reconcile" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolConsumesDecisionContinuation(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "ACCEPTED",
				"decision_summary": "Accepted and needs integration continuation.",
			}))
		case "project.patch_queue.decision_continuation.consume":
			for key, want := range map[string]string{
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"actor_id":     "agent-alpha",
				"queue_id":     "patchq-project-subpixel-projrepo-1",
				"item_id":      "patchitem-branch-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"patch_queue_decision_continuation": map[string]any{
					"outbox_id":            "outbox-accepted-1",
					"workspace_id":         "ws",
					"project_id":           "project-subpixel",
					"queue_id":             "patchq-project-subpixel-projrepo-1",
					"item_id":              "patchitem-branch-1",
					"branch_id":            "branch-1",
					"head_sha":             "head123",
					"decision":             "ACCEPTED",
					"followup_kind":        "integration",
					"continuation_task_id": "task-patchq-integration-project-subpixel-patchitem-branch-1",
					"state":                "CONSUMED",
					"decision_event_id":    "event-decision-1",
					"payload_json":         "{}",
					"created_at":           "2026-05-26T00:00:00Z",
					"updated_at":           "2026-05-26T00:00:01Z",
				},
				"continuation_task": map[string]any{
					"task_id":      "task-patchq-integration-project-subpixel-patchitem-branch-1",
					"workspace_id": "ws",
					"status":       "PENDING",
					"priority":     "high",
					"project_id":   "project-subpixel",
					"project_lane": "integration",
					"created_at":   "2026-05-26T00:00:01Z",
					"updated_at":   "2026-05-26T00:00:01Z",
				},
				"consumed": true,
				"created":  false,
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "consume_continuation",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected consume continuation success, got %+v", result)
	}
	for _, want := range []string{`"action": "consume_continuation"`, `"consumed": true`, `"created": false`, "outbox-accepted-1", "task-patchq-integration-project-subpixel"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.decision_continuation.consume" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolConsumesDecisionContinuationByOutboxID(t *testing.T) {
	calls := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.patch_queue.decision_continuation.consume":
			for key, want := range map[string]string{
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"actor_id":     "agent-alpha",
				"outbox_id":    "outbox-accepted-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got := rpcString(req.Params, "queue_id"); got != "" {
				t.Fatalf("expected queue_id to be omitted for outbox-only consume, got %q", got)
			}
			if got := rpcString(req.Params, "item_id"); got != "" {
				t.Fatalf("expected item_id to be omitted for outbox-only consume, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"patch_queue_decision_continuation": map[string]any{
					"outbox_id":            "outbox-accepted-1",
					"workspace_id":         "ws",
					"project_id":           "project-subpixel",
					"queue_id":             "patchq-project-subpixel-projrepo-1",
					"item_id":              "patchitem-branch-1",
					"branch_id":            "branch-1",
					"head_sha":             "head123",
					"decision":             "ACCEPTED",
					"followup_kind":        "integration",
					"continuation_task_id": "task-patchq-integration-project-subpixel-patchitem-branch-1",
					"state":                "CONSUMED",
					"decision_event_id":    "event-decision-1",
					"payload_json":         "{}",
					"created_at":           "2026-05-26T00:00:00Z",
					"updated_at":           "2026-05-26T00:00:01Z",
				},
				"continuation_task": map[string]any{
					"task_id":      "task-patchq-integration-project-subpixel-patchitem-branch-1",
					"workspace_id": "ws",
					"status":       "PENDING",
					"priority":     "high",
					"project_id":   "project-subpixel",
					"project_lane": "integration",
					"created_at":   "2026-05-26T00:00:01Z",
					"updated_at":   "2026-05-26T00:00:01Z",
				},
				"consumed": true,
				"created":  true,
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "consume_continuation",
		"outbox_id":  "outbox-accepted-1",
		"queue_id":   "stale-queue",
		"item_id":    "stale-item",
		"branch_id":  "stale-branch",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected consume continuation success, got %+v", result)
	}
	for _, want := range []string{`"action": "consume_continuation"`, `"created": true`, `"queue_id": "patchq-project-subpixel-projrepo-1"`, "outbox-accepted-1"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.patch_queue.decision_continuation.consume" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRejectsAmbiguousSelector(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			result := patchQueueCoordinationResult([]map[string]any{
				patchQueueLifecycleItem(map[string]any{"queue_id": "patchq-a", "item_id": "patchitem-collide", "branch_id": "branch-a"}),
				patchQueueLifecycleItem(map[string]any{"queue_id": "patchq-b", "item_id": "patchitem-collide", "branch_id": "branch-b"}),
			})
			coordination := result["coordination"].(map[string]any)
			coordination["roles"] = []map[string]any{{
				"role_id":      "role-integrator",
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"agent_id":     "agent-alpha",
				"role_type":    "INTEGRATOR",
				"status":       "ACTIVE",
			}}
			writeRPCResult(w, req, result)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "reconcile_review_task",
		"item_id":    "patchitem-collide",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "selector is ambiguous") {
		t.Fatalf("expected ambiguous selector error, got %+v", result)
	}
}

func TestProjectPatchQueueLifecycleToolRecordsReviewerEvidence(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":       "CLAIMED",
				"claimed_by":  "agent-alpha",
				"claim_token": "claim-token-1",
			}))
		case "project.patch_queue.reviewer_advisory_record":
			if got := rpcString(req.Params, "claim_token"); got != "claim-token-1" {
				t.Fatalf("unexpected reviewer claim_token %q", got)
			}
			advisory, ok := req.Params["reviewer_advisory"].(map[string]any)
			if !ok || advisory["summary"] != "reviewed rollback and CAS evidence" {
				t.Fatalf("unexpected reviewer advisory payload: %+v", req.Params["reviewer_advisory"])
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":                    "CLAIMED",
				"claimed_by":               "agent-alpha",
				"claim_token":              "claim-token-1",
				"reviewer_advisory_digest": "sha256-reviewer",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	reviewer := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "reviewer_advisory",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"advisory_summary": "reviewed rollback and CAS evidence",
	})
	if reviewer == nil || reviewer.IsError || !strings.Contains(reviewer.Output, `"reviewer_advisory_digest": "sha256-reviewer"`) {
		t.Fatalf("expected reviewer advisory success, got %+v", reviewer)
	}
	operator := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "operator_enablement",
		"queue_id":   "patchq-project-subpixel-projrepo-1",
		"item_id":    "patchitem-branch-1",
	})
	if operator == nil || !operator.IsError || !strings.Contains(operator.Output, "action must be one of claim, release, reconcile_review_task, consume_continuation, reviewer_advisory, accept, reject, block, cancel") {
		t.Fatalf("expected operator enablement to be unavailable to agents, got %+v", operator)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.reviewer_advisory_record" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolDoesNotUseReviewerAdvisoryForOrdinaryControlledReview(t *testing.T) {
	calls := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "CLAIMED",
				"claimed_by":          "agent-alpha",
				"claim_token":         "claim-token-1",
			}))
		default:
			t.Fatalf("ordinary controlled review should not call %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "reviewer_advisory",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"advisory_summary": "reviewed ordinary repair candidate",
	})
	if result == nil || result.IsError ||
		!strings.Contains(result.Output, `"reviewer_advisory_recorded": false`) ||
		!strings.Contains(result.Output, `"next_action": "use_accept_reject_or_block_decision_for_ordinary_lane_review"`) {
		t.Fatalf("expected reviewer_advisory to redirect ordinary review to normal decisions, got %+v", result)
	}
	if len(calls) != 1 || calls[0] != "project.coordination.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRecordsLateDefectAdvisoryForAcceptedItem(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "ACCEPTED",
				"claimed_by":          "",
				"claim_token":         "",
				"head_sha":            "abc123def456",
			}))
		case "project.patch_queue.reviewer_advisory_record":
			if got := rpcString(req.Params, "claim_token"); got != "" {
				t.Fatalf("late accepted advisory should not require claim_token, got %q", got)
			}
			advisory, ok := req.Params["reviewer_advisory"].(map[string]any)
			if !ok {
				t.Fatalf("missing reviewer advisory payload: %+v", req.Params)
			}
			if advisory["scope"] != "lane_correctness" || advisory["verdict"] != "repair_required" || advisory["head_sha"] != "abc123def456" || advisory["defeats_acceptance"] != true {
				t.Fatalf("unexpected late defect advisory payload: %+v", advisory)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":                      "BLOCKED",
				"reviewer_advisory_digest":   "sha256-defect",
				"reviewer_advisory_accepted": true,
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "reviewer_advisory",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"advisory_summary": "Blocking lexer defect; repair required before integration.",
		"advisory_scope":   "lane_correctness",
		"review_doc_key":   "task.review.late-defect.result",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, "defeated the accepted same-head lane candidate") {
		t.Fatalf("expected late defect advisory success, got %+v", result)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.reviewer_advisory_record" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRejectsIntegrationCompletenessAdvisoryAgainstAcceptedLane(t *testing.T) {
	calls := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "ACCEPTED",
				"head_sha":            "abc123def456",
			}))
		default:
			t.Fatalf("mis-scoped accepted advisory should not call %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "reviewer_advisory",
		"queue_id":         "patchq-project-subpixel-projrepo-1",
		"item_id":          "patchitem-branch-1",
		"advisory_summary": "Full product is not assembled yet.",
		"advisory_scope":   "integration_completeness",
		"review_doc_key":   "task.review.full-product.result",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "integration_completeness") {
		t.Fatalf("expected integration-completeness advisory to be refused, got %+v", result)
	}
	if len(calls) != 1 || calls[0] != "project.coordination.get" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolSupersedesBlockedItemWithFreshEvidence(t *testing.T) {
	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "BLOCKED",
				"claimed_by":       "agent-reviewer",
				"claim_token":      "claim-token-1",
				"decision_summary": "Missing browser smoke evidence.",
				"decision_doc_key": "project.project-subpixel.patchq.blocked",
				"decided_by":       "agent-reviewer",
				"decided_at":       "2026-04-28T12:01:00Z",
				"pathset_json":     `{"paths":["src/**","package.json"]}`,
			}))
		case "project.patch_queue.supersede":
			if got := rpcString(req.Params, "item_id"); got != "patchitem-branch-1" {
				t.Fatalf("old item_id = %q, want patchitem-branch-1", got)
			}
			if got := rpcString(req.Params, "new_item_id"); got != "patchitem-branch-1-requeue" {
				t.Fatalf("new_item_id = %q, want patchitem-branch-1-requeue", got)
			}
			if got := rpcString(req.Params, "evidence_doc_key"); got != "task.validation.evidence" {
				t.Fatalf("evidence_doc_key = %q, want task.validation.evidence", got)
			}
			for _, key := range []string{"task_id", "session_id", "run_id", "agent_id", "principal_type", "principal_id"} {
				if _, ok := req.Params[key]; ok {
					t.Fatalf("metadata-only supersede should not send incomplete binding ref %s: %+v", key, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"already_queued": false,
				"patch_queue_item": patchQueueLifecycleItem(map[string]any{
					"item_id":             "patchitem-branch-1-requeue",
					"state":               "PROPOSED",
					"supersedes_queue_id": "patchq-project-subpixel-projrepo-1",
					"supersedes_item_id":  "patchitem-branch-1",
					"evidence_doc_key":    "task.validation.evidence",
					"repo_authority_mode": "repoauthority_controlled_queue",
					"context_digest":      "sha256:successor-context",
					"pathset_json":        `{"paths":["src/**","package.json"]}`,
				}),
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-reviewer")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":         "project-subpixel",
		"action":             "supersede",
		"queue_id":           "patchq-project-subpixel-projrepo-1",
		"item_id":            "patchitem-branch-1",
		"new_item_id":        "patchitem-branch-1-requeue",
		"validation_doc_key": "task.validation.evidence",
	})
	if result == nil || result.IsError ||
		!strings.Contains(result.Output, `"action": "supersede"`) ||
		!strings.Contains(result.Output, `"already_queued": false`) ||
		!strings.Contains(result.Output, `"repo_authority_mode": "repoauthority_controlled_queue"`) ||
		!strings.Contains(result.Output, `"supersedes_item_id": "patchitem-branch-1"`) {
		t.Fatalf("expected supersede success, got %+v", result)
	}
	if strings.Join(calls, ",") != "project.coordination.get,project.patch_queue.supersede" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolClaimsExpiredForeignClaim(t *testing.T) {
	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       "agent-beta",
				"claim_token":      "old-token",
				"claim_expires_at": "2000-01-01T00:00:00Z",
			}))
		case "project.patch_queue.claim":
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":            "CLAIMED",
				"claimed_by":       "agent-alpha",
				"claim_token":      "new-token",
				"claim_expires_at": "2026-04-28T12:00:00Z",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"action":     "claim",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"claim_token": "new-token"`) {
		t.Fatalf("expected expired foreign claim takeover, got %+v", result)
	}
	if len(calls) != 2 || calls[1] != "project.patch_queue.claim" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueLifecycleToolRejectsDecisionWithoutClaim(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueLifecycleCoordinationResult(map[string]any{
			"state": "PROPOSED",
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueLifecycleTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"action":           "accept",
		"branch_id":        "branch-1",
		"decision_summary": "Trying to decide too early.",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "own claim") {
		t.Fatalf("expected own-claim error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no decision RPC, got %d calls", calls)
	}
}

func patchQueueLifecycleCoordinationResult(itemOverrides map[string]any) map[string]any {
	result := patchQueueCoordinationResult([]map[string]any{patchQueueLifecycleItem(itemOverrides)})
	coordination := result["coordination"].(map[string]any)
	coordination["roles"] = []map[string]any{{
		"role_id":      "role-integrator",
		"workspace_id": "ws",
		"project_id":   "project-subpixel",
		"agent_id":     "agent-alpha",
		"role_type":    "INTEGRATOR",
		"status":       "ACTIVE",
	}}
	return result
}

func patchQueueLifecycleItem(overrides map[string]any) map[string]any {
	item := map[string]any{
		"queue_id":            "patchq-project-subpixel-projrepo-1",
		"item_id":             "patchitem-branch-1",
		"workspace_id":        "ws",
		"project_id":          "project-subpixel",
		"repo_id":             "projrepo-1",
		"branch_id":           "branch-1",
		"review_doc_key":      "project.project-subpixel.branch.branch-1.review",
		"repo_authority_mode": "patch_only_temp_repo",
		"state":               "PROPOSED",
		"pathset":             []string{"web/app.js"},
		"base_ref":            "main",
		"base_sha":            "base123",
		"head_sha":            "head123",
		"auto_merge":          false,
	}
	for key, value := range overrides {
		item[key] = value
	}
	return item
}

func TestProjectPatchQueueLifecycleSourceFidelityRemediation(t *testing.T) {
	// CD-1: a fidelity-shaped ACCEPTED rejection is enriched with actionable guidance.
	fidelityErr := errors.New("ACCEPTED source-fidelity review requires rhizome_spec_fidelity_review_v1 or source_fidelity_status: passed")
	hint := projectPatchQueueLifecycleSourceFidelityRemediation(fidelityErr, "ACCEPTED", "project-rq")
	for _, want := range []string{"source_fidelity_status: passed", "checked_source_doc_keys", "BLOCKED_SPEC_DRIFT", projectSourceRefsDocKey("project-rq")} {
		if !strings.Contains(hint, want) {
			t.Fatalf("expected remediation hint to contain %q, got: %q", want, hint)
		}
	}
	if got := projectPatchQueueLifecycleSourceFidelityRemediation(fidelityErr, "BLOCKED", "project-rq"); got != "" {
		t.Fatalf("expected no remediation for non-ACCEPTED decision, got: %q", got)
	}
	if got := projectPatchQueueLifecycleSourceFidelityRemediation(errors.New("network timeout"), "ACCEPTED", "project-rq"); got != "" {
		t.Fatalf("expected no remediation for a non-fidelity error, got: %q", got)
	}
}
