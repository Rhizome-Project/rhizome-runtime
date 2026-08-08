package server

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHandler_ReviewerRouteRequiresExplicitEvidence(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	raw, err := json.Marshal(map[string]any{
		"workspace_id":        "ws1",
		"generator_agent_id":  "agent-gen",
		"available_reviewers": []string{"reviewer-a", "reviewer-b"},
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(context.Background(), raw)
	if rpcErr == nil {
		t.Fatalf("expected explicit-evidence validation error, got result %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params, got %+v", rpcErr)
	}
}

func TestHandler_ReviewerRouteSurfacesHonestAssignment(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-route",
		Title:       "Reviewer Route",
		Description: "Reviewer route contract",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, "ws-route", "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, "ws-route", "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, "ws-route", "reviewer-b", "reviewer")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            "ws-route",
		"bundle_id":               "bundle-1",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"agent-gen", "reviewer-a", "reviewer-a", "reviewer-b"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if !route.Advisory {
		t.Fatalf("expected reviewer route to be explicitly advisory, got %+v", route)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewer == "agent-gen" {
		t.Fatalf("generator must never be reused as near reviewer")
	}
	if route.DedupedCandidateCount != 2 {
		t.Fatalf("expected deduped candidate count 2, got %d", route.DedupedCandidateCount)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerEligibilityBasis != "REGISTERED_ONLINE_TYPED_REVIEWER" {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "collaboration_unobserved" {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_NO_DISTANCE_EVIDENCE" {
		t.Fatalf("expected no-distance-evidence far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial route without evidence-backed far reviewer, got %q", route.IntegrityBand)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if route.RegisteredCandidates != 2 || route.OnlineCandidates != 2 {
		t.Fatalf("expected registered+online candidate counts to reflect workspace evidence, got %+v", route)
	}
}

func TestHandler_ReviewerRouteSurfacesSelectedFarReviewerStatus(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-far-selected"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Far Selected",
		Description: "Reviewer route should surface selected far reviewer status",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "agent-near", "agent-far1", "agent-far2")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "agent-near", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "agent-far1", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "agent-far2", "reviewer")

	insertReviewerMeshMemoryNode(t, ctx, store, workspaceID, "m1", "agent-gen", "hash-a", "origin-1")
	insertReviewerMeshMemoryNode(t, ctx, store, workspaceID, "m2", "agent-gen", "hash-b", "origin-2")
	insertReviewerMeshMemoryNode(t, ctx, store, workspaceID, "m3", "agent-gen", "hash-common", "origin-3")
	insertReviewerMeshMemoryNode(t, ctx, store, workspaceID, "m4", "agent-far1", "hash-common", "origin-4")
	insertReviewerMeshMemoryNode(t, ctx, store, workspaceID, "m5", "agent-far2", "hash-unique", "origin-5")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-far-selected",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"agent-near", "agent-far1", "agent-far2"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.FarReviewer != "agent-far2" {
		t.Fatalf("expected far reviewer with strongest pairwise distance, got %q", route.FarReviewer)
	}
	if route.FarReviewerStatus != "SELECTED_DISTANCE_EVIDENCE" {
		t.Fatalf("expected selected far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.FarReviewerBasis != "pairwise_distance" {
		t.Fatalf("expected pairwise-distance far reviewer basis, got %q", route.FarReviewerBasis)
	}
}

func TestHandler_ReviewerRouteUsesOnlyWorkspaceOnlineEvidence(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-evidence"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Evidence",
		Description: "Reviewer route should not trust caller-only candidates",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-offline", "reviewer-online")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-offline", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-online", "reviewer")
	setReviewerMeshRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-offline", "2026-04-08T00:00:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-evidence",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-unregistered", "reviewer-offline", "reviewer-online"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "typed_reviewer_candidate_omitted_without_workspace_session_collaboration" {
		t.Fatalf("expected omitted typed reviewer basis, got %q", route.NearReviewerBasis)
	}
	if route.RegisteredCandidates != 2 || route.OnlineCandidates != 1 {
		t.Fatalf("expected registered=2 online=1, got %+v", route)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_candidates_missing_workspace_registration") {
		t.Fatalf("expected missing-registration warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_candidates_offline") {
		t.Fatalf("expected offline-candidate warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRouteFailsClosedWithoutWorkspaceOnlineCandidates(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-no-online"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route No Online",
		Description: "Reviewer route should stay partial without online candidates",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-offline")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-offline", "reviewer")
	setReviewerMeshRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-offline", "2026-04-08T00:00:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-no-online",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-unregistered", "reviewer-offline"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected no near reviewer without workspace-online candidates, got %q", route.NearReviewer)
	}
	if route.NearReviewerBasis != "no_workspace_online_candidate" {
		t.Fatalf("expected no-workspace-online basis, got %q", route.NearReviewerBasis)
	}
	if route.NearReviewerEligibilityBasis != "NO_EVIDENCE_BACKED_CANDIDATE" {
		t.Fatalf("expected no-candidate eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "no_workspace_online_candidate" {
		t.Fatalf("expected no-workspace-online fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_NO_EVIDENCE_BACKED_CANDIDATES" {
		t.Fatalf("expected no-evidence-candidates far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.RegisteredCandidates != 1 || route.OnlineCandidates != 0 {
		t.Fatalf("expected registered=1 online=0, got %+v", route)
	}
}

func TestHandler_ReviewerRouteSuppressesConcreteReviewerWhenScarcitySaturated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-scarcity-saturated"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Scarcity Saturated",
		Description: "Reviewer route should suppress concrete assignment under saturated scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-12T12:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-saturated", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-saturated", "coal-saturated", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-saturated", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-saturated", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "session-saturated", "2026-04-12T12:01:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-saturated",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected concrete near reviewer to be suppressed under saturated scarcity, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_REVIEWER_SCARCITY_SATURATED" {
		t.Fatalf("expected scarcity-saturated near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerFallbackReason != "reviewer_scarcity_saturated" {
		t.Fatalf("expected scarcity-saturated fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "reviewer_scarcity_saturated") {
		t.Fatalf("expected reviewer scarcity saturation warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRouteMarksHeuristicOmissionAsScarcitySaturated(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-scarcity-saturated-heuristic"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Scarcity Saturated Heuristic",
		Description: "Reviewer route should mark heuristic omission as scarcity-saturated under reviewer saturation",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-12T12:05:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-saturated-heuristic", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-saturated-heuristic", "coal-saturated-heuristic", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-saturated-heuristic", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-saturated-heuristic", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-saturated-heuristic",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" || route.FarReviewer != "" {
		t.Fatalf("expected no concrete reviewers under saturated scarcity, got near=%q far=%q", route.NearReviewer, route.FarReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_REVIEWER_SCARCITY_SATURATED" {
		t.Fatalf("expected scarcity-saturated near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "saturated_reviewer_mesh" {
		t.Fatalf("expected saturated reviewer mesh near reviewer basis, got %q", route.NearReviewerBasis)
	}
	if route.NearReviewerFallbackReason != "reviewer_scarcity_saturated" {
		t.Fatalf("expected scarcity-saturated fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_REVIEWER_SCARCITY_SATURATED" {
		t.Fatalf("expected scarcity-saturated far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.FarReviewerBasis != "saturated_reviewer_mesh" {
		t.Fatalf("expected saturated reviewer mesh far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if !slices.Contains(route.Warnings, "reviewer_scarcity_saturated") {
		t.Fatalf("expected reviewer scarcity saturation warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRouteDowngradesWhenGeneratorIsOffline(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-generator-offline"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Generator Offline",
		Description: "Reviewer route should stay partial when generator is offline",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentLastSeen(t, ctx, store, workspaceID, "agent-gen", "2026-04-08T00:00:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-generator-offline",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial route when generator is offline, got %q", route.IntegrityBand)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "generator_agent_offline") {
		t.Fatalf("expected offline-generator warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRoutePrefersLowerLiveReviewerLoad(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-load-aware"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Load Aware",
		Description: "Reviewer route should prefer lower live reviewer load",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-busy", "reviewer-light", "reviewer-other")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-light", "reviewer")

	liveAt := "2026-04-09T13:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-load-a", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-load-a", "coal-load-a", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-load-a", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-load-a", "reviewer-busy", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-load-b", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-load-b", "coal-load-b", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-load-b", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-load-b", "reviewer-busy", "FAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-load-aware",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-busy", "reviewer-light", "reviewer-other"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "typed_reviewer_candidate_omitted_without_workspace_session_collaboration" {
		t.Fatalf("expected omitted typed reviewer basis, got %q", route.NearReviewerBasis)
	}
	if route.NearReviewerEligibilityBasis != "REGISTERED_ONLINE_TYPED_REVIEWER" {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "collaboration_unobserved" {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_NO_DISTANCE_EVIDENCE" {
		t.Fatalf("expected no-distance-evidence far reviewer status, got %q", route.FarReviewerStatus)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_typed_eligibility") {
		t.Fatalf("expected mixed typed-eligibility warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRoutePrefersTypedReviewerEligibilityWhenPresent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-typed-eligibility"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Typed Eligibility",
		Description: "Typed reviewer candidates should outrank generalists",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "generalist-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-typed-eligibility",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"generalist-a", "reviewer-b"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.TypedEligibleCandidates != 1 {
		t.Fatalf("expected one typed eligible candidate, got %d", route.TypedEligibleCandidates)
	}
	if route.NearReviewerEligibilityBasis != "REGISTERED_ONLINE_TYPED_REVIEWER" {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "collaboration_unobserved" {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_NO_DISTANCE_EVIDENCE" {
		t.Fatalf("expected no-distance-evidence far reviewer status, got %q", route.FarReviewerStatus)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_typed_eligibility") {
		t.Fatalf("expected mixed typed-eligibility warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRoutePrefersObservedSessionCollaborationWithinTypedPool(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-handler-collaboration"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Collaboration",
		Description: "Reviewer route should prefer observed session collaboration within a typed pool",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-b", "session-handler-collaboration", "2026-04-09T16:00:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            workspaceID,
		"bundle_id":               "bundle-handler-collaboration",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-a", "reviewer-b"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if !route.Advisory {
		t.Fatalf("expected reviewer route to remain advisory, got %+v", route)
	}
	if route.NearReviewer != "reviewer-b" {
		t.Fatalf("expected collaboration-observed reviewer to win, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "SELECTED_COLLABORATION_EVIDENCE" {
		t.Fatalf("expected collaboration-evidence near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.ObservedCollaborationCandidates != 1 || route.NearReviewerObservedCollaboration != 1 {
		t.Fatalf("expected collaboration counts to surface in response, got %+v", route)
	}
	if route.NearReviewerEligibilityBasis != "REGISTERED_ONLINE_TYPED_REVIEWER" {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "" {
		t.Fatalf("expected no fallback reason with observed typed collaboration, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_INSUFFICIENT_EVIDENCE_BACKED_CANDIDATES" && route.FarReviewerStatus != "OMITTED_NO_DISTANCE_EVIDENCE" {
		t.Fatalf("expected omitted far reviewer status on two-candidate collaboration path, got %q", route.FarReviewerStatus)
	}
	if route.NearReviewerBasis != "typed_reviewer_eligible_observed_workspace_session_collaboration_then_live_reviewer_load_then_candidate_order" {
		t.Fatalf("expected collaboration-aware basis, got %q", route.NearReviewerBasis)
	}
	if !slices.Contains(route.Warnings, "reviewer_collaboration_workspace_scoped") || !slices.Contains(route.Warnings, "near_reviewer_collaboration_workspace_scoped") {
		t.Fatalf("expected workspace-scoped collaboration warnings, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_collaboration_evidence") {
		t.Fatalf("expected mixed collaboration evidence warning, got %v", route.Warnings)
	}
	if slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("did not expect heuristic near reviewer warning with observed collaboration, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRouteIgnoresCrossWorkspaceCollaborationDemand(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const targetWorkspaceID = "ws-route-handler-collaboration-target"
	const otherWorkspaceID = "ws-route-handler-collaboration-other"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: targetWorkspaceID,
		Title:       "Reviewer Route Collaboration Target",
		Description: "Target workspace",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create target workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, targetWorkspaceID)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: otherWorkspaceID,
		Title:       "Reviewer Route Collaboration Other",
		Description: "Other workspace",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, otherWorkspaceID)
	registerReviewerMeshRouteAgents(t, ctx, store, targetWorkspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	registerReviewerMeshRouteAgents(t, ctx, store, otherWorkspaceID, "agent-gen", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, targetWorkspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, targetWorkspaceID, "reviewer-b", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, otherWorkspaceID, "reviewer-b", "reviewer")
	recordReviewerMeshDecisionDemand(t, ctx, store, otherWorkspaceID, "agent-gen", "reviewer-b", "session-handler-cross-workspace", "2026-04-09T16:05:00Z")

	raw, err := json.Marshal(map[string]any{
		"workspace_id":            targetWorkspaceID,
		"bundle_id":               "bundle-handler-cross-workspace",
		"generator_agent_id":      "agent-gen",
		"available_reviewers":     []string{"reviewer-a", "reviewer-b"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.reviewerRoute(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerRoute rpc error: %+v", rpcErr)
	}
	route := result.(sqlite.VerifierMeshRoute)
	if route.NearReviewer != "" {
		t.Fatalf("expected no concrete near reviewer when cross-workspace evidence is ignored, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.ObservedCollaborationCandidates != 0 || route.NearReviewerObservedCollaboration != 0 {
		t.Fatalf("expected no cross-workspace collaboration counts, got %+v", route)
	}
	if route.NearReviewerEligibilityBasis != "REGISTERED_ONLINE_TYPED_REVIEWER" {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "collaboration_unobserved" {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != "OMITTED_NO_DISTANCE_EVIDENCE" {
		t.Fatalf("expected no-distance-evidence far reviewer status, got %q", route.FarReviewerStatus)
	}
	if !slices.Contains(route.Warnings, "reviewer_collaboration_unobserved") {
		t.Fatalf("expected collaboration-unobserved warning, got %v", route.Warnings)
	}
}

func TestHandler_ReviewerRouteOmitsNearReviewerOnEqualTypedTieRegardlessOfCallerOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-route-equal-typed-tie"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Route Equal Typed Tie",
		Description: "Near reviewer should be omitted on equal typed tie",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	for _, candidateOrder := range [][]string{{"reviewer-a", "reviewer-b"}, {"reviewer-b", "reviewer-a"}} {
		raw, err := json.Marshal(map[string]any{
			"workspace_id":            workspaceID,
			"bundle_id":               "bundle-equal-typed-tie",
			"generator_agent_id":      "agent-gen",
			"available_reviewers":     candidateOrder,
			"is_multi_patch":          false,
			"impact_score":            0.2,
			"contradiction_pressure":  0.1,
			"has_active_dissent":      false,
			"touches_hard_constraint": false,
			"cluster_mode":            "explore",
			"merge_risk":              0.2,
		})
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}

		result, rpcErr := h.reviewerRoute(ctx, raw)
		if rpcErr != nil {
			t.Fatalf("reviewerRoute rpc error for order %v: %+v", candidateOrder, rpcErr)
		}
		route := result.(sqlite.VerifierMeshRoute)
		if route.NearReviewer != "" {
			t.Fatalf("expected no concrete near reviewer on equal typed tie for order %v, got %q", candidateOrder, route.NearReviewer)
		}
		if route.NearReviewerStatus != "OMITTED_HEURISTIC_ONLY" {
			t.Fatalf("expected heuristic-only omission on equal typed tie for order %v, got %q", candidateOrder, route.NearReviewerStatus)
		}
	}
}

func TestHandler_ReviewerScarcityUsesPersistedLoad(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity",
		Description: "Reviewer scarcity contract",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-c", "reviewer")

	liveAt := "2026-04-09T10:00:00Z"
	for _, tensionID := range []string{"tension-a", "tension-b", "tension-c"} {
		insertReviewerMeshTension(t, ctx, store, workspaceID, tensionID, liveAt)
	}

	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-a", "coal-a", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-a", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-b", "coal-b", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-b", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-b", "reviewer-b", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-c", "coal-c", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-c", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-c", "reviewer-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "SATURATED" {
		t.Fatalf("expected SATURATED reviewer scarcity, got %q", snapshot.Status)
	}
	if snapshot.OnlineAgents != 4 || snapshot.OnlineTypedReviewers != 3 || snapshot.ActiveReviewerAssignments != 3 {
		t.Fatalf("expected persisted scarcity counts, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != "ONLINE_TYPED_REVIEWER_UPPER_BOUND" || snapshot.CapacityUpperBound != 3 {
		t.Fatalf("expected typed reviewer capacity basis, got %+v", snapshot)
	}
	if snapshot.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity band because capacity remains an upper bound, got %q", snapshot.IntegrityBand)
	}
}

func TestHandler_ReviewerScarcityAvoidsFalseHealthyWithoutActiveReviewerLoad(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-empty-load"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Empty Load",
		Description: "Reviewer scarcity should stay honest without load evidence",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")

	liveAt := "2026-04-09T11:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-empty", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-empty", "coal-empty", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-empty", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN reviewer scarcity without active reviewer load, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "no_active_reviewer_load") || !slices.Contains(snapshot.Reasons, "live_coalitions_without_reviewer_assignments") {
		t.Fatalf("expected honest no-load reasons, got %+v", snapshot.Reasons)
	}
	if !slices.Contains(snapshot.Reasons, "session_collaboration_load_unobserved") {
		t.Fatalf("expected collaboration-load-unobserved reason, got %+v", snapshot.Reasons)
	}
}

func TestHandler_ReviewerScarcityAvoidsHealthyWithUpperBoundOnlyCapacity(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-upper-bound"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Upper Bound",
		Description: "Low utilization should stay unknown under upper-bound-only capacity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")

	liveAt := "2026-04-09T12:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-upper-bound", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-upper-bound", "coal-upper-bound", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-upper-bound", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-upper-bound", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN scarcity under low utilization with upper-bound-only capacity, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "capacity_upper_bound_only_low_utilization") {
		t.Fatalf("expected low-utilization upper-bound reason, got %+v", snapshot.Reasons)
	}
}

func TestHandler_ReviewerScarcityUsesTypedReviewerCapacityUpperBound(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-typed-capacity"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Typed Capacity",
		Description: "Typed reviewer capacity should bound scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "generalist-b", "generalist-c")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-10T10:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-typed-capacity", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-typed-capacity", "coal-typed-capacity", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-capacity", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-capacity", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "SATURATED" {
		t.Fatalf("expected SATURATED scarcity under typed reviewer saturation, got %+v", snapshot)
	}
	if snapshot.OnlineAgents != 4 || snapshot.OnlineTypedReviewers != 1 || snapshot.CapacityUpperBound != 1 {
		t.Fatalf("expected typed reviewer upper bound, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != "ONLINE_TYPED_REVIEWER_UPPER_BOUND" {
		t.Fatalf("expected typed reviewer capacity basis, got %+v", snapshot)
	}
}

func TestHandler_ReviewerScarcityKeepsFallbackUpperBoundUnknownWithoutOnlineTypedReviewers(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-fallback-unknown"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Fallback Unknown",
		Description: "Generalist fallback should stay unknown without online typed reviewers",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "generalist-c")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerMeshRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-a", "2000-01-01T00:00:00Z")
	setReviewerMeshRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-b", "2000-01-01T00:00:00Z")

	liveAt := "2026-04-10T11:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-fallback-unknown", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-fallback-unknown", "coal-fallback-unknown", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-fallback-unknown", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-fallback-unknown", "generalist-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN scarcity when only fallback upper bound remains, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "generalist_fallback_reviewer_assignments_observed") {
		t.Fatalf("expected fallback reason, got %+v", snapshot.Reasons)
	}
}

func TestHandler_ReviewerScarcityDoesNotCountGeneralistFallbackAgainstTypedHeadroom(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-typed-headroom"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Typed Headroom",
		Description: "Generalist fallback should not consume typed reviewer headroom",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen-a", "agent-gen-b", "typed-a", "typed-b", "generalist-c")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "typed-a", "reviewer")
	setReviewerMeshRouteAgentRole(t, ctx, store, workspaceID, "typed-b", "reviewer")

	liveAt := "2026-04-10T11:30:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-typed-headroom-a", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-typed-headroom-a", "coal-typed-headroom-a", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-headroom-a", "agent-gen-a", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-headroom-a", "typed-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-typed-headroom-b", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-typed-headroom-b", "coal-typed-headroom-b", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-headroom-b", "agent-gen-b", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-typed-headroom-b", "generalist-c", "FAR_REVIEWER", 0.8, 0.6, 4, liveAt)

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "SCARCE" {
		t.Fatalf("expected SCARCE with live fallback reviewer while typed headroom remains, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != "ONLINE_TYPED_REVIEWER_UPPER_BOUND" || snapshot.CapacityUpperBound != 2 || snapshot.AvailableHeadroom != 1 {
		t.Fatalf("expected typed headroom to remain visible, got %+v", snapshot)
	}
}

func TestHandler_ReviewerScarcityFlagsHighlyConcentratedReviewerLoad(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-scarcity-concentrated"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Concentrated",
		Description: "Concentrated reviewer load should surface scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerReviewerMeshScarcityAgents(t, ctx, store, workspaceID, "agent-gen-a", "agent-gen-b", "reviewer-a", "reviewer-b", "reviewer-c")

	liveAt := "2026-04-09T14:00:00Z"
	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-concentrated-a", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-concentrated-a", "coal-concentrated-a", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-concentrated-a", "agent-gen-a", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-concentrated-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen-a", "reviewer-a", "session-handler-concentrated-a", "2026-04-09T14:00:00Z")

	insertReviewerMeshTension(t, ctx, store, workspaceID, "tension-concentrated-b", liveAt)
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, "tension-concentrated-b", "coal-concentrated-b", "ACTIVE", 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-concentrated-b", "agent-gen-b", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertReviewerMeshMember(t, ctx, store, workspaceID, "coal-concentrated-b", "reviewer-a", "FAR_REVIEWER", 0.8, 0.6, 4, liveAt)
	recordReviewerMeshDecisionDemand(t, ctx, store, workspaceID, "agent-gen-b", "reviewer-a", "session-handler-concentrated-b", "2026-04-09T14:01:00Z")

	raw, err := json.Marshal(map[string]any{"workspace_id": workspaceID})
	if err != nil {
		t.Fatalf("marshal scarcity params: %v", err)
	}

	result, rpcErr := h.reviewerScarcity(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("reviewerScarcity rpc error: %+v", rpcErr)
	}
	snapshot := result.(sqlite.ReviewerMeshScarcitySnapshot)
	if snapshot.Status != "SCARCE" {
		t.Fatalf("expected concentrated reviewer load to surface SCARCE, got %+v", snapshot)
	}
	if snapshot.ReviewerLoadHHI != 1 {
		t.Fatalf("expected reviewer load HHI of 1, got %f", snapshot.ReviewerLoadHHI)
	}
	if !slices.Contains(snapshot.Reasons, "reviewer_load_highly_concentrated") {
		t.Fatalf("expected concentrated-load reason, got %+v", snapshot.Reasons)
	}
	if snapshot.OpenSessionCollaborationAssignments != 2 || snapshot.DistinctSessionCollaborationReviewers != 1 {
		t.Fatalf("expected session collaboration load to surface, got %+v", snapshot)
	}
	if snapshot.SessionCollaborationLoadHHI != 1 {
		t.Fatalf("expected session collaboration load HHI of 1, got %f", snapshot.SessionCollaborationLoadHHI)
	}
	if !slices.Contains(snapshot.Reasons, "session_collaboration_load_workspace_scoped") {
		t.Fatalf("expected workspace-scoped collaboration-load reason, got %+v", snapshot.Reasons)
	}
	if !slices.Contains(snapshot.Reasons, "session_collaboration_load_highly_concentrated") {
		t.Fatalf("expected concentrated session-collaboration reason, got %+v", snapshot.Reasons)
	}
}

func insertReviewerMeshTension(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, now string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]', ?, ?)`,
		tensionID,
		workspaceID,
		"cluster-"+tensionID,
		"review_scarcity",
		"ACTIVE",
		"PENDING",
		"Reviewer scarcity tension",
		"",
		now,
		now,
	); err != nil {
		t.Fatalf("insert reviewer mesh tension %s: %v", tensionID, err)
	}
}

func insertReviewerMeshCoalition(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, coalitionID, status string, createdEpoch int, createdAt string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalitions (
			coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		coalitionID,
		workspaceID,
		tensionID,
		"Reviewer Mesh Surface",
		0.75,
		3,
		status,
		createdEpoch,
		createdAt,
		createdAt,
	); err != nil {
		t.Fatalf("insert reviewer mesh coalition %s: %v", coalitionID, err)
	}
}

func insertReviewerMeshMember(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, coalitionID, agentID, role string, fitScore, noveltyScore float64, minStayUntilEpoch int, joinedAt string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalition_members (
			coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		coalitionID,
		workspaceID,
		agentID,
		role,
		fitScore,
		noveltyScore,
		minStayUntilEpoch,
		joinedAt,
	); err != nil {
		t.Fatalf("insert reviewer mesh member %s for %s: %v", agentID, coalitionID, err)
	}
}

func registerReviewerMeshRouteAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "operator",
			DisplayName: agentID,
			Status:      "ACTIVE",
		}); err != nil {
			t.Fatalf("register reviewer route agent %s: %v", agentID, err)
		}
		if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Status:      "ACTIVE",
			Summary:     "reviewer mesh route test presence",
		}); err != nil {
			t.Fatalf("heartbeat reviewer route agent %s: %v", agentID, err)
		}
	}
}

func registerReviewerMeshScarcityAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	registerReviewerMeshRouteAgents(t, ctx, store, workspaceID, agentIDs...)
}

func setReviewerMeshRouteAgentLastSeen(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, lastSeen string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`,
		lastSeen,
		lastSeen,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("set reviewer route agent last_seen_at for %s: %v", agentID, err)
	}
}

func setReviewerMeshRouteAgentRole(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, agentID, role string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET role = ?, updated_at = COALESCE(last_seen_at, updated_at) WHERE workspace_id = ? AND agent_id = ?`,
		role,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("set reviewer route agent role for %s: %v", agentID, err)
	}
}

func insertReviewerMeshMemoryNode(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID, agentID, sourceID, originID string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO memory_nodes (
			workspace_id, memory_id, agent_id, source_kind, source_id, memory_type, visibility, memory_layer,
			epistemic_status, lifecycle_state, origin_kind, origin_id, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspaceID,
		memoryID,
		agentID,
		"doc",
		sourceID,
		"OBS",
		"PUBLIC",
		"L2",
		"KNOWN",
		"ACTIVE",
		"task",
		originID,
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert reviewer mesh memory node %s: %v", memoryID, err)
	}
}

func recordReviewerMeshDecisionDemand(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, generatorAgentID, reviewerAgentID, sessionID, updatedAt string) {
	t.Helper()
	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:          model.SessionEventDecisionNeeded,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		AgentID:            generatorAgentID,
		Summary:            "Need reviewer decision",
		Status:             model.SessionStatusWaitingDecision,
		DecisionNeededFrom: reviewerAgentID,
		DecisionType:       "review",
		UpdatedAt:          updatedAt,
	})
	if err != nil {
		t.Fatalf("record reviewer mesh decision demand: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
		t.Fatalf("sync reviewer mesh decision demand: %v", err)
	}
}
