package sqlite

import (
	"context"
	"slices"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestRouteVerification_Level1(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	mustCreateReviewerWorkspace(t, ctx, store, "ws-1", "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, "ws-1", "agent-gen", "rev-a", "rev-b", "rev-c")
	setReviewerRouteAgentRole(t, ctx, store, "ws-1", "rev-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, "ws-1", "rev-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, "ws-1", "rev-c", "reviewer")

	input := VerifierMeshRouteInput{
		WorkspaceID:           "ws-1",
		BundleID:              "bundle-1",
		IsMultiPatch:          false,
		ImpactScore:           0.2,
		ContradictionPressure: 0.1,
		HasActiveDissent:      false,
		TouchesHardConstraint: false,
		ClusterMode:           "explore",
		MergeRisk:             0.1,
		GeneratorAgentID:      "agent-gen",
		AvailableReviewers:    []string{"rev-a", "rev-b", "rev-c"},
	}

	route, err := store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}

	if route.Level != VerifierLevel1 {
		t.Fatalf("expected Level 1, got %d", route.Level)
	}
	if route.NearReviewer != "" || route.FarReviewer != "" {
		t.Fatalf("expected no concrete reviewers without evidence-backed collaboration or distance, got near=%s far=%s", route.NearReviewer, route.FarReviewer)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity without evidence-backed far reviewer, got %q", route.IntegrityBand)
	}
	if route.RegisteredCandidates != 3 || route.OnlineCandidates != 3 {
		t.Fatalf("expected all reviewers to be registered+online, got registered=%d online=%d", route.RegisteredCandidates, route.OnlineCandidates)
	}
	if route.TypedEligibleCandidates != 3 {
		t.Fatalf("expected all reviewers to be typed-eligible, got %d", route.TypedEligibleCandidates)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackCollaborationUnobserved {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if route.FarReviewerBasis != "no_evidence_backed_candidate" {
		t.Fatalf("expected no-evidence far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if route.FarReviewerStatus != farReviewerStatusOmittedNoDistanceEvidence {
		t.Fatalf("expected no-distance-evidence far reviewer status, got %q", route.FarReviewerStatus)
	}
}

func TestRouteVerification_FarReviewerSelection(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	workspaceID := "ws-far-rev"
	agentGen := "agent-gen"
	agentNear := "agent-near"
	agentFar1 := "agent-far1"
	agentFar2 := "agent-far2"
	agentFar3 := "agent-far3"

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Test",
		Description: "Test",
		CreatedBy:   agentGen,
	}); err != nil {
		t.Fatal(err)
	}
	registerReviewerRouteAgents(t, ctx, store, workspaceID, agentGen, agentNear, agentFar1, agentFar2, agentFar3)
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, agentNear, "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, agentFar1, "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, agentFar2, "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, agentFar3, "reviewer")

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	execOrFatal := func(q string, args ...any) {
		if _, err := tx.Exec(q, args...); err != nil {
			t.Fatalf("db insert failed: %v", err)
		}
	}

	insertQ := "INSERT INTO memory_nodes (workspace_id, memory_id, agent_id, source_kind, source_id, memory_type, visibility, memory_layer, epistemic_status, lifecycle_state, origin_kind, origin_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertProps := []any{"OBS", "PUBLIC", "L2", "KNOWN", "ACTIVE", "task"}

	execOrFatal(insertQ, append(append([]any{workspaceID, "m1", agentGen, "doc", "hash-a"}, insertProps...), "origin-1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")...)
	execOrFatal(insertQ, append(append([]any{workspaceID, "m2", agentGen, "doc", "hash-b"}, insertProps...), "origin-2", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")...)
	execOrFatal(insertQ, append(append([]any{workspaceID, "m3", agentGen, "doc", "hash-common"}, insertProps...), "origin-3", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")...)
	execOrFatal(insertQ, append(append([]any{workspaceID, "m4", agentFar1, "doc", "hash-common"}, insertProps...), "origin-4", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")...)
	execOrFatal(insertQ, append(append([]any{workspaceID, "m5", agentFar2, "doc", "hash-unique"}, insertProps...), "origin-5", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")...)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit test memory nodes: %v", err)
	}

	input := VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   agentGen,
		AvailableReviewers: []string{agentNear, agentFar3, agentFar1, agentFar2},
	}

	route, err := store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity because near reviewer lacks collaboration evidence, got %q", route.IntegrityBand)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if route.FarReviewer != agentFar2 {
		t.Fatalf("expected highest distance reviewer %q to win, got %q", agentFar2, route.FarReviewer)
	}
	if route.FarReviewerBasis != "pairwise_distance" {
		t.Fatalf("expected pairwise-distance far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if route.FarReviewerStatus != farReviewerStatusSelectedDistanceEvidence {
		t.Fatalf("expected selected far reviewer status, got %q", route.FarReviewerStatus)
	}

	input.AvailableReviewers = []string{agentNear, agentFar3}
	route, err = store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed on no-evidence path: %v", err)
	}
	if route.FarReviewer != "" {
		t.Fatalf("expected no far reviewer when only no-evidence candidate remains, got %q", route.FarReviewer)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity on no-evidence path, got %q", route.IntegrityBand)
	}
	if route.FarReviewerStatus != farReviewerStatusOmittedNoDistanceEvidence {
		t.Fatalf("expected no-distance-evidence far reviewer status on no-evidence path, got %q", route.FarReviewerStatus)
	}

	input.AvailableReviewers = []string{agentNear, agentFar3, agentFar1}
	route, err = store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed on mixed-evidence path: %v", err)
	}
	if route.FarReviewer != agentFar1 {
		t.Fatalf("expected evidence-backed reviewer %q to beat no-evidence reviewer %q, got %q", agentFar1, agentFar3, route.FarReviewer)
	}
	if route.FarReviewerStatus != farReviewerStatusSelectedDistanceEvidence {
		t.Fatalf("expected selected far reviewer status on mixed-evidence path, got %q", route.FarReviewerStatus)
	}
}

func TestRouteVerification_Level2(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	mustCreateReviewerWorkspace(t, ctx, store, "ws-2", "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, "ws-2", "agent-gen", "rev-a")
	setReviewerRouteAgentRole(t, ctx, store, "ws-2", "rev-a", "reviewer")

	input := VerifierMeshRouteInput{
		WorkspaceID:           "ws-2",
		BundleID:              "bundle-2",
		IsMultiPatch:          true,
		ImpactScore:           0.2,
		ContradictionPressure: 0.1,
		HasActiveDissent:      false,
		TouchesHardConstraint: false,
		ClusterMode:           "explore",
		MergeRisk:             0.1,
		GeneratorAgentID:      "agent-gen",
		AvailableReviewers:    []string{"rev-a"},
	}

	route, err := store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.Level != VerifierLevel2 {
		t.Fatalf("expected Level 2, got %d", route.Level)
	}
	if route.NearReviewer != "" || route.FarReviewer != "" {
		t.Fatalf("unexpected reviewer assignment near=%s far=%s", route.NearReviewer, route.FarReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity with only one evidence-backed reviewer, got %q", route.IntegrityBand)
	}
	if route.FarReviewerBasis != "insufficient_evidence_backed_candidates" {
		t.Fatalf("expected insufficient-evidence far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if route.FarReviewerStatus != farReviewerStatusOmittedInsufficientEvidenceCandidates {
		t.Fatalf("expected insufficient-candidates far reviewer status, got %q", route.FarReviewerStatus)
	}
}

func TestRouteVerification_Level3(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	mustCreateReviewerWorkspace(t, ctx, store, "ws-3", "agent-gen")

	input := VerifierMeshRouteInput{
		WorkspaceID:           "ws-3",
		BundleID:              "bundle-3",
		IsMultiPatch:          false,
		ImpactScore:           0.5,
		ContradictionPressure: 0.1,
		HasActiveDissent:      true,
		TouchesHardConstraint: false,
		ClusterMode:           "explore",
		MergeRisk:             0.1,
		GeneratorAgentID:      "agent-gen",
	}

	route, err := store.RouteVerification(ctx, input)
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.Level != VerifierLevel3 {
		t.Fatalf("expected Level 3, got %d", route.Level)
	}
	if len(route.Reasons) == 0 || route.Reasons[0] != "active_dissent_present" {
		t.Fatalf("missing correct reason, got: %v", route.Reasons)
	}
}

func TestRouteVerification_ExcludesGeneratorAndDedupesCandidates(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	mustCreateReviewerWorkspace(t, ctx, store, "ws-generator-dedupe", "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, "ws-generator-dedupe", "agent-gen", "rev-a", "rev-b")
	setReviewerRouteAgentRole(t, ctx, store, "ws-generator-dedupe", "rev-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, "ws-generator-dedupe", "rev-b", "reviewer")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        "ws-generator-dedupe",
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"agent-gen", "rev-a", "rev-a", "rev-b"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewer == "agent-gen" {
		t.Fatalf("generator must never be reused as near reviewer")
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.DedupedCandidateCount != 2 {
		t.Fatalf("expected deduped non-generator candidate count = 2, got %d", route.DedupedCandidateCount)
	}
	if len(route.Warnings) == 0 {
		t.Fatalf("expected warning for deduped/generator-pruned candidate pool")
	}
}

func TestRouteVerification_UsesOnlyWorkspaceOnlineReviewerEvidence(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-evidence"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-offline", "reviewer-online")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-offline", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-online", "reviewer")
	setReviewerRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-offline", "2026-04-08T00:00:00Z")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-unregistered", "reviewer-offline", "reviewer-online"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "typed_reviewer_candidate_omitted_without_workspace_session_collaboration" {
		t.Fatalf("expected omitted typed reviewer basis, got %q", route.NearReviewerBasis)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected PARTIAL integrity with unregistered/offline candidates, got %q", route.IntegrityBand)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if route.RegisteredCandidates != 2 || route.OnlineCandidates != 1 {
		t.Fatalf("expected registered=2 online=1, got registered=%d online=%d", route.RegisteredCandidates, route.OnlineCandidates)
	}
	if !slices.Contains(route.Warnings, "reviewer_candidates_missing_workspace_registration") {
		t.Fatalf("expected missing-registration warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_candidates_offline") {
		t.Fatalf("expected offline-candidate warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_FailsClosedWhenNoWorkspaceOnlineReviewerExists(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-no-online"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-offline")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-offline", "reviewer")
	setReviewerRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-offline", "2026-04-08T00:00:00Z")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-unregistered", "reviewer-offline"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected no near reviewer without workspace-online candidates, got %q", route.NearReviewer)
	}
	if route.NearReviewerBasis != "no_workspace_online_candidate" {
		t.Fatalf("expected no-workspace-online basis, got %q", route.NearReviewerBasis)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityNone {
		t.Fatalf("expected no-candidate eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackNoWorkspaceOnlineCandidate {
		t.Fatalf("expected no-workspace-online fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.FarReviewerStatus != farReviewerStatusOmittedNoEvidenceCandidates {
		t.Fatalf("expected no-evidence-candidates far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.FarReviewerBasis != "no_evidence_backed_candidate" {
		t.Fatalf("expected no-evidence far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected PARTIAL integrity without workspace-online candidates, got %q", route.IntegrityBand)
	}
	if route.RegisteredCandidates != 1 || route.OnlineCandidates != 0 {
		t.Fatalf("expected registered=1 online=0, got registered=%d online=%d", route.RegisteredCandidates, route.OnlineCandidates)
	}
}

func TestRouteVerification_SuppressesConcreteReviewerWhenScarcitySaturated(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-scarcity-saturated"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-12T12:00:00Z"
	insertReviewerRouteLoadCoalition(t, ctx, store, workspaceID, "tension-saturated", "coal-saturated", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-saturated", "agent-gen", "GENERATOR", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-saturated", "reviewer-a", "NEAR_REVIEWER", liveAt)
	recordReviewerRouteDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "session-saturated", "2026-04-12T12:01:00Z")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:           workspaceID,
		GeneratorAgentID:      "agent-gen",
		AvailableReviewers:    []string{"reviewer-a"},
		IsMultiPatch:          false,
		ImpactScore:           0.2,
		ContradictionPressure: 0.1,
		HasActiveDissent:      false,
		TouchesHardConstraint: false,
		ClusterMode:           "explore",
		MergeRisk:             0.1,
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected concrete near reviewer to be suppressed under saturated reviewer scarcity, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedScarcitySaturated {
		t.Fatalf("expected scarcity-saturated near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackScarcitySaturated {
		t.Fatalf("expected scarcity-saturated fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "reviewer_scarcity_saturated") {
		t.Fatalf("expected reviewer scarcity saturation warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Reasons, "reviewer_scarcity_saturated") {
		t.Fatalf("expected reviewer scarcity saturation reason, got %v", route.Reasons)
	}
}

func TestRouteVerification_MarksHeuristicOmissionAsScarcitySaturated(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-scarcity-saturated-heuristic"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-12T12:05:00Z"
	insertReviewerRouteLoadCoalition(t, ctx, store, workspaceID, "tension-saturated-heuristic", "coal-saturated-heuristic", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-saturated-heuristic", "agent-gen", "GENERATOR", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-saturated-heuristic", "reviewer-a", "NEAR_REVIEWER", liveAt)

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:           workspaceID,
		GeneratorAgentID:      "agent-gen",
		AvailableReviewers:    []string{"reviewer-a"},
		IsMultiPatch:          false,
		ImpactScore:           0.2,
		ContradictionPressure: 0.1,
		HasActiveDissent:      false,
		TouchesHardConstraint: false,
		ClusterMode:           "explore",
		MergeRisk:             0.1,
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" || route.FarReviewer != "" {
		t.Fatalf("expected no concrete reviewers under saturated scarcity, got near=%q far=%q", route.NearReviewer, route.FarReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedScarcitySaturated {
		t.Fatalf("expected scarcity-saturated near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "saturated_reviewer_mesh" {
		t.Fatalf("expected saturated reviewer mesh near reviewer basis, got %q", route.NearReviewerBasis)
	}
	if route.FarReviewerStatus != farReviewerStatusOmittedScarcitySaturated {
		t.Fatalf("expected scarcity-saturated far reviewer status, got %q", route.FarReviewerStatus)
	}
	if route.FarReviewerBasis != "saturated_reviewer_mesh" {
		t.Fatalf("expected saturated reviewer mesh far reviewer basis, got %q", route.FarReviewerBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackScarcitySaturated {
		t.Fatalf("expected scarcity-saturated near reviewer fallback reason, got %q", route.NearReviewerFallbackReason)
	}
}

func TestRouteVerification_DowngradesWhenGeneratorLacksWorkspaceEvidence(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-generator-missing"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "operator")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "reviewer-online")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-online", "reviewer")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "ghost-generator",
		AvailableReviewers: []string{"reviewer-online"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected PARTIAL integrity when generator lacks workspace evidence, got %q", route.IntegrityBand)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "generator_agent_missing_workspace_registration") {
		t.Fatalf("expected missing-generator warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_PrefersLowerLiveReviewerLoad(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-load-aware"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-busy", "reviewer-light", "reviewer-other")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-light", "reviewer")

	liveAt := "2026-04-09T13:00:00Z"
	insertReviewerRouteLoadCoalition(t, ctx, store, workspaceID, "tension-load-a", "coal-load-a", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-load-a", "agent-gen", "GENERATOR", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-load-a", "reviewer-busy", "NEAR_REVIEWER", liveAt)

	insertReviewerRouteLoadCoalition(t, ctx, store, workspaceID, "tension-load-b", "coal-load-b", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-load-b", "agent-gen", "GENERATOR", liveAt)
	insertReviewerRouteLoadMember(t, ctx, store, workspaceID, "coal-load-b", "reviewer-busy", "FAR_REVIEWER", liveAt)

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-busy", "reviewer-light", "reviewer-other"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerBasis != "typed_reviewer_candidate_omitted_without_workspace_session_collaboration" {
		t.Fatalf("expected omitted typed reviewer basis, got %q", route.NearReviewerBasis)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("expected near reviewer heuristic warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_typed_eligibility") {
		t.Fatalf("expected mixed typed-eligibility warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_PrefersTypedReviewerEligibilityWhenPresent(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-typed-eligibility"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "generalist-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"generalist-a", "reviewer-b"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only near reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.TypedEligibleCandidates != 1 {
		t.Fatalf("expected one typed eligible candidate, got %d", route.TypedEligibleCandidates)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackCollaborationUnobserved {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_typed_eligibility") {
		t.Fatalf("expected mixed typed-eligibility warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_WarnsWhenNoTypedReviewerEligibilityExists(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-no-typed-eligibility"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "generalist-a")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"generalist-a"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected heuristic-only generalist reviewer to be omitted, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.TypedEligibleCandidates != 0 {
		t.Fatalf("expected zero typed eligible candidates, got %d", route.TypedEligibleCandidates)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityGeneralistFallback {
		t.Fatalf("expected generalist fallback eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackTypedEligibilityUnavailable {
		t.Fatalf("expected typed-eligibility-unavailable fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if route.NearReviewerBasis != "generalist_candidate_omitted_without_workspace_session_collaboration" {
		t.Fatalf("expected omitted generalist reviewer basis, got %q", route.NearReviewerBasis)
	}
	if !slices.Contains(route.Warnings, "reviewer_candidates_missing_typed_eligibility") {
		t.Fatalf("expected missing typed-eligibility warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_PrefersObservedSessionCollaborationWithinTypedPool(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-collaboration"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	recordReviewerRouteDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "reviewer-b", "session-collab-a", "2026-04-09T14:00:00Z")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-a", "reviewer-b"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if !route.Advisory {
		t.Fatalf("expected reviewer route to stay advisory, got %+v", route)
	}
	if route.NearReviewer != "reviewer-b" {
		t.Fatalf("expected collaboration-observed reviewer to beat same-tier peer, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusSelectedCollaborationEvidence {
		t.Fatalf("expected collaboration-evidence near reviewer status, got %q", route.NearReviewerStatus)
	}
	if route.ObservedCollaborationCandidates != 1 {
		t.Fatalf("expected one collaboration-observed candidate, got %d", route.ObservedCollaborationCandidates)
	}
	if route.NearReviewerObservedCollaboration != 1 {
		t.Fatalf("expected selected near reviewer to carry collaboration evidence, got %d", route.NearReviewerObservedCollaboration)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != "" {
		t.Fatalf("expected no fallback reason when typed collaboration is observed, got %q", route.NearReviewerFallbackReason)
	}
	if route.NearReviewerBasis != "typed_reviewer_eligible_observed_workspace_session_collaboration_then_live_reviewer_load_then_candidate_order" {
		t.Fatalf("expected collaboration-aware near reviewer basis, got %q", route.NearReviewerBasis)
	}
	if !slices.Contains(route.Reasons, "near_reviewer_collaboration_observed") {
		t.Fatalf("expected observed collaboration reason, got %v", route.Reasons)
	}
	if !slices.Contains(route.Warnings, "reviewer_collaboration_workspace_scoped") || !slices.Contains(route.Warnings, "near_reviewer_collaboration_workspace_scoped") {
		t.Fatalf("expected workspace-scoped collaboration warnings, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "reviewer_pool_mixed_collaboration_evidence") {
		t.Fatalf("expected mixed collaboration warning, got %v", route.Warnings)
	}
	if slices.Contains(route.Warnings, "near_reviewer_heuristic_only") {
		t.Fatalf("did not expect heuristic-only warning when collaboration evidence exists, got %v", route.Warnings)
	}
}

func TestRouteVerification_WarnsWhenNoObservedSessionCollaborationExists(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-collaboration-missing"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        workspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-a"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.ObservedCollaborationCandidates != 0 || route.NearReviewerObservedCollaboration != 0 {
		t.Fatalf("expected no observed collaboration counts, got %+v", route)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected no concrete near reviewer without collaboration evidence, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackCollaborationUnobserved {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "reviewer_collaboration_unobserved") {
		t.Fatalf("expected global collaboration-unobserved warning, got %v", route.Warnings)
	}
	if !slices.Contains(route.Warnings, "near_reviewer_collaboration_unobserved") {
		t.Fatalf("expected near reviewer collaboration-unobserved warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_IgnoresCrossWorkspaceCollaborationDemand(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const targetWorkspaceID = "ws-route-collaboration-target"
	const otherWorkspaceID = "ws-route-collaboration-other"
	mustCreateReviewerWorkspace(t, ctx, store, targetWorkspaceID, "agent-gen")
	mustCreateReviewerWorkspace(t, ctx, store, otherWorkspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, targetWorkspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	registerReviewerRouteAgents(t, ctx, store, otherWorkspaceID, "agent-gen", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, targetWorkspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, targetWorkspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, otherWorkspaceID, "reviewer-b", "reviewer")
	recordReviewerRouteDecisionDemand(t, ctx, store, otherWorkspaceID, "agent-gen", "reviewer-b", "session-cross-workspace", "2026-04-09T15:00:00Z")

	route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
		WorkspaceID:        targetWorkspaceID,
		GeneratorAgentID:   "agent-gen",
		AvailableReviewers: []string{"reviewer-a", "reviewer-b"},
	})
	if err != nil {
		t.Fatalf("RouteVerification failed: %v", err)
	}
	if route.NearReviewer != "" {
		t.Fatalf("expected no concrete near reviewer when cross-workspace evidence is ignored, got %q", route.NearReviewer)
	}
	if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
		t.Fatalf("expected heuristic-only near reviewer omission, got %q", route.NearReviewerStatus)
	}
	if route.ObservedCollaborationCandidates != 0 || route.NearReviewerObservedCollaboration != 0 {
		t.Fatalf("expected no collaboration evidence to leak across workspaces, got %+v", route)
	}
	if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
		t.Fatalf("expected typed reviewer eligibility basis, got %q", route.NearReviewerEligibilityBasis)
	}
	if route.NearReviewerFallbackReason != nearReviewerFallbackCollaborationUnobserved {
		t.Fatalf("expected collaboration-unobserved fallback reason, got %q", route.NearReviewerFallbackReason)
	}
	if !slices.Contains(route.Warnings, "reviewer_collaboration_unobserved") {
		t.Fatalf("expected collaboration-unobserved warning, got %v", route.Warnings)
	}
}

func TestRouteVerification_OmitsNearReviewerOnEqualTypedTieRegardlessOfCallerOrder(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-route-equal-typed-tie"
	mustCreateReviewerWorkspace(t, ctx, store, workspaceID, "agent-gen")
	registerReviewerRouteAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")

	for _, candidateOrder := range [][]string{{"reviewer-a", "reviewer-b"}, {"reviewer-b", "reviewer-a"}} {
		route, err := store.RouteVerification(ctx, VerifierMeshRouteInput{
			WorkspaceID:        workspaceID,
			GeneratorAgentID:   "agent-gen",
			AvailableReviewers: candidateOrder,
		})
		if err != nil {
			t.Fatalf("RouteVerification failed for order %v: %v", candidateOrder, err)
		}
		if route.NearReviewer != "" {
			t.Fatalf("expected no concrete near reviewer on equal typed tie for order %v, got %q", candidateOrder, route.NearReviewer)
		}
		if route.NearReviewerStatus != nearReviewerStatusOmittedHeuristicOnly {
			t.Fatalf("expected heuristic-only omission on equal typed tie for order %v, got %q", candidateOrder, route.NearReviewerStatus)
		}
		if route.NearReviewerEligibilityBasis != nearReviewerEligibilityTypedReviewer {
			t.Fatalf("expected typed reviewer eligibility basis on equal typed tie for order %v, got %q", candidateOrder, route.NearReviewerEligibilityBasis)
		}
	}
}

func TestReviewerMeshScarcitySnapshotReflectsPersistedLoad(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity",
		Description: "Reviewer scarcity surface",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-c", "reviewer")

	liveAt := "2026-04-09T10:00:00Z"
	for _, tensionID := range []string{"tension-a", "tension-b", "tension-c"} {
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
			liveAt,
			liveAt,
		); err != nil {
			t.Fatalf("insert tension %s: %v", tensionID, err)
		}
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-a", "coal-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-a", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-b", "coal-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-b", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-b", "reviewer-b", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-c", "coal-c", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-c", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-c", "reviewer-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "SATURATED" {
		t.Fatalf("expected SATURATED under typed reviewer saturation pressure, got %q", snapshot.Status)
	}
	if snapshot.OnlineAgents != 4 || snapshot.OnlineTypedReviewers != 3 || snapshot.CapacityUpperBound != 3 {
		t.Fatalf("expected typed reviewer upper bound of 3 over 4 online agents, got %+v", snapshot)
	}
	if snapshot.DistinctActiveReviewers != 3 || snapshot.ActiveReviewerAssignments != 3 {
		t.Fatalf("expected reviewer load to reflect persisted coalition roles, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != reviewerCapacityBasisOnlineTypedReviewerUpperBound {
		t.Fatalf("expected typed reviewer capacity basis, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "capacity_uses_online_typed_reviewer_upper_bound") || !slices.Contains(snapshot.Reasons, "generalist_online_agents_excluded_from_typed_capacity") {
		t.Fatalf("expected typed-capacity reasons, got %+v", snapshot.Reasons)
	}
	if snapshot.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity band because capacity is still an upper bound, got %q", snapshot.IntegrityBand)
	}
}

func TestReviewerMeshScarcitySnapshotAvoidsFalseHealthyWithoutActiveReviewerLoad(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-empty-load"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Empty Load",
		Description: "Reviewer scarcity should not go healthy without load evidence",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b")

	liveAt := "2026-04-09T11:00:00Z"
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]', ?, ?)`,
		"tension-empty",
		workspaceID,
		"cluster-empty",
		"review_scarcity",
		"ACTIVE",
		"PENDING",
		"Reviewer scarcity empty load",
		"",
		liveAt,
		liveAt,
	); err != nil {
		t.Fatalf("insert tension: %v", err)
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-empty", "coal-empty", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-empty", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN reviewer scarcity without active reviewer load, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "no_active_reviewer_load") || !slices.Contains(snapshot.Reasons, "live_coalitions_without_reviewer_assignments") {
		t.Fatalf("expected no-load reasons in scarcity snapshot, got %+v", snapshot.Reasons)
	}
	if !slices.Contains(snapshot.Reasons, "session_collaboration_load_unobserved") {
		t.Fatalf("expected collaboration-load-unobserved reason, got %+v", snapshot.Reasons)
	}
	if snapshot.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected partial integrity band to remain explicit, got %q", snapshot.IntegrityBand)
	}
}

func TestReviewerMeshScarcitySnapshotAvoidsHealthyWithUpperBoundOnlyCapacity(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-upper-bound"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Upper Bound",
		Description: "Low utilization should stay unknown when capacity is only an upper bound",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "reviewer-c")

	liveAt := "2026-04-09T12:00:00Z"
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]', ?, ?)`,
		"tension-upper-bound",
		workspaceID,
		"cluster-upper-bound",
		"review_scarcity",
		"ACTIVE",
		"PENDING",
		"Reviewer scarcity upper bound",
		"",
		liveAt,
		liveAt,
	); err != nil {
		t.Fatalf("insert tension: %v", err)
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-upper-bound", "coal-upper-bound", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-upper-bound", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-upper-bound", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN scarcity under low utilization with upper-bound-only capacity, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "capacity_upper_bound_only_low_utilization") {
		t.Fatalf("expected low-utilization upper-bound reason, got %+v", snapshot.Reasons)
	}
	if snapshot.DistinctActiveReviewers != 1 || snapshot.CapacityUpperBound != 4 {
		t.Fatalf("expected 1 active reviewer over online upper bound of 4, got %+v", snapshot)
	}
}

func TestReviewerMeshScarcitySnapshotUsesTypedReviewerCapacityUpperBound(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-typed-capacity"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Typed Capacity",
		Description: "Typed reviewer capacity should dominate scarcity upper bound",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "generalist-b", "generalist-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")

	liveAt := "2026-04-10T10:00:00Z"
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json, agent_ids_json, constraint_refs_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]', ?, ?)`,
		"tension-typed-capacity",
		workspaceID,
		"cluster-typed-capacity",
		"review_scarcity",
		"ACTIVE",
		"PENDING",
		"Reviewer scarcity typed capacity",
		"",
		liveAt,
		liveAt,
	); err != nil {
		t.Fatalf("insert tension: %v", err)
	}
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-typed-capacity", "coal-typed-capacity", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-capacity", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-capacity", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "SATURATED" {
		t.Fatalf("expected SATURATED when one typed reviewer fills typed capacity, got %+v", snapshot)
	}
	if snapshot.OnlineAgents != 4 || snapshot.OnlineTypedReviewers != 1 || snapshot.CapacityUpperBound != 1 {
		t.Fatalf("expected typed reviewer capacity basis to reduce upper bound, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != reviewerCapacityBasisOnlineTypedReviewerUpperBound {
		t.Fatalf("expected typed reviewer capacity basis, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "capacity_uses_online_typed_reviewer_upper_bound") || !slices.Contains(snapshot.Reasons, "generalist_online_agents_excluded_from_typed_capacity") {
		t.Fatalf("expected typed-capacity reasons, got %+v", snapshot.Reasons)
	}
}

func TestReviewerMeshScarcitySnapshotKeepsFallbackUpperBoundUnknownWithoutOnlineTypedReviewers(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-fallback-unknown"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Fallback Unknown",
		Description: "Generalist fallback should not overclaim scarcity when typed reviewers are offline",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "reviewer-a", "reviewer-b", "generalist-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "reviewer-b", "reviewer")
	setReviewerRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-a", "2000-01-01T00:00:00Z")
	setReviewerRouteAgentLastSeen(t, ctx, store, workspaceID, "reviewer-b", "2000-01-01T00:00:00Z")

	liveAt := "2026-04-10T11:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-fallback-unknown", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-fallback-unknown", "coal-fallback-unknown", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-fallback-unknown", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-fallback-unknown", "generalist-c", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "UNKNOWN" {
		t.Fatalf("expected UNKNOWN scarcity when only fallback upper bound remains, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "no_online_typed_reviewers") || !slices.Contains(snapshot.Reasons, "generalist_fallback_reviewer_assignments_observed") {
		t.Fatalf("expected explicit fallback reasons, got %+v", snapshot.Reasons)
	}
}

func TestReviewerMeshScarcitySnapshotDoesNotCountGeneralistFallbackAgainstTypedHeadroom(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-typed-headroom"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Typed Headroom",
		Description: "Generalist fallback should not consume typed headroom",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen-a", "agent-gen-b", "typed-a", "typed-b", "generalist-c")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "typed-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "typed-b", "reviewer")

	liveAt := "2026-04-10T11:30:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-typed-headroom-a", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-typed-headroom-a", "coal-typed-headroom-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-headroom-a", "agent-gen-a", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-headroom-a", "typed-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)

	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-typed-headroom-b", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-typed-headroom-b", "coal-typed-headroom-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-headroom-b", "agent-gen-b", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-typed-headroom-b", "generalist-c", "FAR_REVIEWER", 0.8, 0.6, 4, liveAt)

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "SCARCE" {
		t.Fatalf("expected SCARCE because fallback reviewer is active while typed headroom remains, got %+v", snapshot)
	}
	if snapshot.CapacityBasis != reviewerCapacityBasisOnlineTypedReviewerUpperBound || snapshot.CapacityUpperBound != 2 {
		t.Fatalf("expected typed capacity upper bound of 2, got %+v", snapshot)
	}
	if snapshot.AvailableHeadroom != 1 {
		t.Fatalf("expected one typed reviewer of headroom to remain, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "typed_capacity_excludes_generalist_fallback_assignments") {
		t.Fatalf("expected explicit typed-headroom reason, got %+v", snapshot.Reasons)
	}
}

func TestReviewerMeshScarcitySnapshotIgnoresNonReviewerSessionCollaborationLoad(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-non-reviewer-collab"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Non Reviewer Collaboration",
		Description: "Non-reviewer collaboration assignments should not inflate reviewer scarcity",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen", "typed-a", "typed-b")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "typed-a", "reviewer")
	setReviewerRouteAgentRole(t, ctx, store, workspaceID, "typed-b", "reviewer")

	liveAt := "2026-04-10T12:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-non-reviewer-collab", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-non-reviewer-collab", "coal-non-reviewer-collab", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-non-reviewer-collab", "agent-gen", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-non-reviewer-collab", "typed-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	recordReviewerRouteDecisionDemand(t, ctx, store, workspaceID, "agent-gen", "human-operator", "session-human-collab", "2026-04-10T12:01:00Z")

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.OpenSessionCollaborationAssignments != 0 {
		t.Fatalf("expected non-reviewer collaboration assignments to be ignored, got %+v", snapshot)
	}
	if !slices.Contains(snapshot.Reasons, "non_capacity_session_collaboration_assignments_ignored") {
		t.Fatalf("expected ignored non-reviewer collaboration reason, got %+v", snapshot.Reasons)
	}
}

func TestReviewerMeshScarcitySnapshotFlagsHighlyConcentratedReviewerLoad(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	const workspaceID = "ws-reviewer-scarcity-concentrated"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Reviewer Scarcity Concentrated",
		Description: "Concentrated reviewer load should surface scarcity even below headcount thresholds",
		CreatedBy:   "operator",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	registerReviewerScarcityAgents(t, ctx, store, workspaceID, "agent-gen-a", "agent-gen-b", "reviewer-a", "reviewer-b", "reviewer-c")

	liveAt := "2026-04-09T14:00:00Z"
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-concentrated-a", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-concentrated-a", "coal-concentrated-a", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-concentrated-a", "agent-gen-a", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-concentrated-a", "reviewer-a", "NEAR_REVIEWER", 0.8, 0.3, 4, liveAt)
	recordReviewerRouteDecisionDemand(t, ctx, store, workspaceID, "agent-gen-a", "reviewer-a", "session-concentrated-a", "2026-04-09T14:00:00Z")

	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, "tension-concentrated-b", liveAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, "tension-concentrated-b", "coal-concentrated-b", "ACTIVE", 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-concentrated-b", "agent-gen-b", "GENERATOR", 0.9, 0.4, 4, liveAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, "coal-concentrated-b", "reviewer-a", "FAR_REVIEWER", 0.8, 0.6, 4, liveAt)
	recordReviewerRouteDecisionDemand(t, ctx, store, workspaceID, "agent-gen-b", "reviewer-a", "session-concentrated-b", "2026-04-09T14:01:00Z")

	snapshot, err := store.ReviewerMeshScarcitySnapshot(ctx, workspaceID)
	if err != nil {
		t.Fatalf("ReviewerMeshScarcitySnapshot failed: %v", err)
	}
	if snapshot.Status != "SCARCE" {
		t.Fatalf("expected concentrated reviewer load to surface SCARCE, got %+v", snapshot)
	}
	if snapshot.ReviewerLoadHHI != 1 {
		t.Fatalf("expected reviewer load HHI of 1 for fully concentrated load, got %f", snapshot.ReviewerLoadHHI)
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
	if snapshot.DistinctActiveReviewers != 1 || snapshot.ActiveReviewerAssignments != 2 || snapshot.CapacityUpperBound != 5 {
		t.Fatalf("expected 1 distinct active reviewer across 2 assignments and 5 online agents, got %+v", snapshot)
	}
}

func mustCreateReviewerWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID, createdBy string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		Description: workspaceID,
		CreatedBy:   createdBy,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
}

func registerReviewerRouteAgents(t *testing.T, ctx context.Context, store *Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "operator",
			DisplayName: agentID,
			Status:      "ACTIVE",
		}); err != nil {
			t.Fatalf("register reviewer route agent %s: %v", agentID, err)
		}
		if err := store.RecordAgentHeartbeat(ctx, AgentHeartbeatInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			Status:      "ACTIVE",
			Summary:     "reviewer route test presence",
		}); err != nil {
			t.Fatalf("heartbeat reviewer route agent %s: %v", agentID, err)
		}
	}
}

func registerReviewerScarcityAgents(t *testing.T, ctx context.Context, store *Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	registerReviewerRouteAgents(t, ctx, store, workspaceID, agentIDs...)
}

func setReviewerRouteAgentLastSeen(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, lastSeen string) {
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

func setReviewerRouteAgentRole(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, role string) {
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

func insertReviewerRouteLoadCoalition(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, coalitionID, createdAt string) {
	t.Helper()
	insertReviewerRouteLoadTension(t, ctx, store, workspaceID, tensionID, createdAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, coalitionID, "ACTIVE", 4, createdAt)
}

func insertReviewerRouteLoadTension(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, createdAt string) {
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
		"review_route_load",
		"ACTIVE",
		"PENDING",
		"Reviewer Route Load",
		"",
		createdAt,
		createdAt,
	); err != nil {
		t.Fatalf("insert reviewer route load tension %s: %v", tensionID, err)
	}
}

func insertReviewerRouteLoadMember(t *testing.T, ctx context.Context, store *Store, workspaceID, coalitionID, agentID, role, joinedAt string) {
	t.Helper()
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, coalitionID, agentID, role, 0.8, 0.3, 4, joinedAt)
}

func recordReviewerRouteDecisionDemand(t *testing.T, ctx context.Context, store *Store, workspaceID, generatorAgentID, reviewerAgentID, sessionID, updatedAt string) {
	t.Helper()
	state, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
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
		t.Fatalf("record reviewer route decision demand: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, state); err != nil {
		t.Fatalf("sync reviewer route decision demand: %v", err)
	}
}
