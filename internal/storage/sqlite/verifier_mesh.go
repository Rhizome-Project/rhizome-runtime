package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	VerifierLevel1 = 1 // Fast (Syntax, schema, basic consistency)
	VerifierLevel2 = 2 // Medium (Local dependencies, constraints, cross-reference)
	VerifierLevel3 = 3 // Hard (Formal verification, exhaustive search, simulation)
)

// VerifierMeshRouteInput provides the contextual signals required to determine
// the verification tier and reviewer assignment for a patch bundle.
type VerifierMeshRouteInput struct {
	WorkspaceID           string
	BundleID              string
	IsMultiPatch          bool
	ImpactScore           float64 // 0.0 to 1.0
	ContradictionPressure float64 // 0.0 to 1.0
	HasActiveDissent      bool
	TouchesHardConstraint bool
	ClusterMode           string  // e.g., "stabilize", "explore"
	MergeRisk             float64 // 0.0 to 1.0

	// Candidate pool for assignment
	AvailableReviewers []string
	GeneratorAgentID   string
}

// VerifierMeshRoute represents the output of the Verifier Mesh engine.
type VerifierMeshRoute struct {
	Advisory                          bool     `json:"advisory"`
	Level                             int      `json:"level"`
	NearReviewerStatus                string   `json:"near_reviewer_status,omitempty"`
	NearReviewer                      string   `json:"near_reviewer,omitempty"`
	FarReviewer                       string   `json:"far_reviewer,omitempty"`
	Reasons                           []string `json:"reasons"`
	IntegrityBand                     string   `json:"integrity_band,omitempty"`
	Warnings                          []string `json:"warnings,omitempty"`
	DedupedCandidateCount             int      `json:"deduped_candidate_count,omitempty"`
	RegisteredCandidates              int      `json:"registered_candidates,omitempty"`
	OnlineCandidates                  int      `json:"online_candidates,omitempty"`
	TypedEligibleCandidates           int      `json:"typed_eligible_candidates,omitempty"`
	ObservedCollaborationCandidates   int      `json:"observed_collaboration_candidates,omitempty"`
	NearReviewerObservedCollaboration int      `json:"near_reviewer_observed_collaboration_count,omitempty"`
	NearReviewerEligibilityBasis      string   `json:"near_reviewer_eligibility_basis,omitempty"`
	NearReviewerFallbackReason        string   `json:"near_reviewer_fallback_reason,omitempty"`
	NearReviewerBasis                 string   `json:"near_reviewer_basis,omitempty"`
	FarReviewerStatus                 string   `json:"far_reviewer_status,omitempty"`
	FarReviewerBasis                  string   `json:"far_reviewer_basis,omitempty"`
}

const (
	nearReviewerStatusSelectedCollaborationEvidence = "SELECTED_COLLABORATION_EVIDENCE"
	nearReviewerStatusOmittedHeuristicOnly          = "OMITTED_HEURISTIC_ONLY"
	nearReviewerStatusOmittedNoEvidenceCandidates   = "OMITTED_NO_EVIDENCE_BACKED_CANDIDATE"
	nearReviewerStatusOmittedScarcitySaturated      = "OMITTED_REVIEWER_SCARCITY_SATURATED"

	nearReviewerEligibilityTypedReviewer      = "REGISTERED_ONLINE_TYPED_REVIEWER"
	nearReviewerEligibilityGeneralistFallback = "REGISTERED_ONLINE_GENERALIST_FALLBACK"
	nearReviewerEligibilityNone               = "NO_EVIDENCE_BACKED_CANDIDATE"

	nearReviewerFallbackTypedEligibilityUnavailable = "typed_eligibility_unavailable"
	nearReviewerFallbackCollaborationUnobserved     = "collaboration_unobserved"
	nearReviewerFallbackNoWorkspaceOnlineCandidate  = "no_workspace_online_candidate"
	nearReviewerFallbackScarcitySaturated           = "reviewer_scarcity_saturated"

	farReviewerStatusSelectedDistanceEvidence              = "SELECTED_DISTANCE_EVIDENCE"
	farReviewerStatusOmittedNoDistanceEvidence             = "OMITTED_NO_DISTANCE_EVIDENCE"
	farReviewerStatusOmittedInsufficientEvidenceCandidates = "OMITTED_INSUFFICIENT_EVIDENCE_BACKED_CANDIDATES"
	farReviewerStatusOmittedNoEvidenceCandidates           = "OMITTED_NO_EVIDENCE_BACKED_CANDIDATES"
	farReviewerStatusOmittedScarcitySaturated              = "OMITTED_REVIEWER_SCARCITY_SATURATED"
)

type ReviewerMeshScarcitySnapshot struct {
	WorkspaceID                           string   `json:"workspace_id"`
	Status                                string   `json:"status"` // UNKNOWN | SCARCE | SATURATED
	IntegrityBand                         string   `json:"integrity_band"`
	EvidenceSource                        string   `json:"evidence_source"`
	CapacityBasis                         string   `json:"capacity_basis"`
	RegisteredAgents                      int      `json:"registered_agents"`
	OnlineAgents                          int      `json:"online_agents"`
	RegisteredTypedReviewers              int      `json:"registered_typed_reviewers"`
	OnlineTypedReviewers                  int      `json:"online_typed_reviewers"`
	CapacityUpperBound                    int      `json:"capacity_upper_bound"`
	LiveCoalitions                        int      `json:"live_coalitions"`
	ActiveReviewerAssignments             int      `json:"active_reviewer_assignments"`
	ActiveTypedReviewerAssignments        int      `json:"active_typed_reviewer_assignments"`
	ActiveGeneralistFallbackAssignments   int      `json:"active_generalist_fallback_assignments"`
	DistinctActiveReviewers               int      `json:"distinct_active_reviewers"`
	ReviewerLoadHHI                       float64  `json:"reviewer_load_hhi"`
	OpenSessionCollaborationAssignments   int      `json:"open_session_collaboration_assignments"`
	DistinctSessionCollaborationReviewers int      `json:"distinct_session_collaboration_reviewers"`
	SessionCollaborationLoadHHI           float64  `json:"session_collaboration_load_hhi"`
	AvailableHeadroom                     int      `json:"available_headroom"`
	CoalitionsMissingFarReview            int      `json:"coalitions_missing_far_reviewer"`
	Reasons                               []string `json:"reasons,omitempty"`
}

const reviewerFarDistanceThreshold = 0.60
const reviewerLoadConcentrationScarceThreshold = 0.85

const (
	reviewerCapacityBasisOnlineTypedReviewerUpperBound = "ONLINE_TYPED_REVIEWER_UPPER_BOUND"
	reviewerCapacityBasisOnlineAgentUpperBoundFallback = "ONLINE_AGENT_UPPER_BOUND_NO_TYPED_REVIEWERS"
)

func normalizeReviewerCandidates(generatorAgentID string, reviewers []string) []string {
	generatorAgentID = strings.TrimSpace(generatorAgentID)
	seen := make(map[string]struct{}, len(reviewers))
	out := make([]string, 0, len(reviewers))
	for _, reviewer := range reviewers {
		reviewer = strings.TrimSpace(reviewer)
		if reviewer == "" || reviewer == generatorAgentID {
		 continue
		}
		if _, ok := seen[reviewer]; ok {
		 continue
		}
		seen[reviewer] = struct{}{}
		out = append(out, reviewer)
	}
	return out
}

type reviewerCandidatePresence struct {
	registered    bool
	online        bool
	typedEligible bool
}

func reviewerCandidateIsTypedEligible(role string, capabilities []string) bool {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "REVIEWER", "NEAR_REVIEWER", "FAR_REVIEWER", "VERIFIER":
		return true
	}
	for _, capability := range capabilities {
		capability = strings.ToLower(strings.TrimSpace(capability))
		if capability == "" {
		 continue
		}
		if strings.HasPrefix(capability, "review") || strings.HasPrefix(capability, "verify") {
		 return true
		}
	}
	return false
}

func (s *Store) filterEvidenceBackedReviewerCandidates(ctx context.Context, workspaceID string, candidates []string) ([]string, int, int, int, map[string]reviewerCandidatePresence, error) {
	if len(candidates) == 0 {
		return nil, 0, 0, 0, nil, nil
	}

	byAgentID, _, _, _, _, err := s.reviewerAgentPresence(ctx, workspaceID)
	if err != nil {
		return nil, 0, 0, 0, nil, err
	}

	registeredCandidates := 0
	onlineCandidates := 0
	typedEligibleCandidates := 0
	evidenceBacked := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		presence, ok := byAgentID[candidate]
		if !ok || !presence.registered {
		 continue
		}
		registeredCandidates++
		if !presence.online {
		 continue
		}
		onlineCandidates++
		if presence.typedEligible {
		 typedEligibleCandidates++
		}
		evidenceBacked = append(evidenceBacked, candidate)
	}

	return evidenceBacked, registeredCandidates, onlineCandidates, typedEligibleCandidates, byAgentID, nil
}

func (s *Store) reviewerAgentPresence(ctx context.Context, workspaceID string) (map[string]reviewerCandidatePresence, int, int, int, int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT agent_id, role, capabilities_json, last_seen_at
		 FROM agents
		 WHERE workspace_id = ?`,
		workspaceID,
	)
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("query reviewer agents: %w", err)
	}
	defer rows.Close()

	byAgentID := make(map[string]reviewerCandidatePresence)
	registeredAgents := 0
	onlineAgents := 0
	registeredTypedReviewers := 0
	onlineTypedReviewers := 0
	for rows.Next() {
		var agentID string
		var role string
		var capabilitiesJSON string
		var lastSeen sql.NullString
		if err := rows.Scan(&agentID, &role, &capabilitiesJSON, &lastSeen); err != nil {
		 return nil, 0, 0, 0, 0, fmt.Errorf("scan reviewer agent: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
		 continue
		}
		capabilities := decodeCapabilities(capabilitiesJSON)
		presence := reviewerCandidatePresence{
		 registered:    true,
		 online:        computeIsOnline(nullStringPtr(lastSeen)),
		 typedEligible: reviewerCandidateIsTypedEligible(role, capabilities),
		}
		byAgentID[agentID] = presence
		registeredAgents++
		if presence.online {
		 onlineAgents++
		}
		if presence.typedEligible {
		 registeredTypedReviewers++
		 if presence.online {
		 onlineTypedReviewers++
		 }
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("iterate reviewer agents: %w", err)
	}
	return byAgentID, registeredAgents, onlineAgents, registeredTypedReviewers, onlineTypedReviewers, nil
}

func (s *Store) reviewerLiveAssignmentCounts(ctx context.Context, workspaceID string) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT m.agent_id, COUNT(1)
		 FROM workspace_coalition_members m
		 JOIN workspace_coalitions c
		 ON c.workspace_id = m.workspace_id AND c.coalition_id = m.coalition_id
		 WHERE c.workspace_id = ?
		 AND c.status IN ('FORMING', 'ACTIVE')
		 AND m.role IN ('NEAR_REVIEWER', 'FAR_REVIEWER')
		 GROUP BY m.agent_id`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query reviewer live assignment counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var agentID string
		var count int
		if err := rows.Scan(&agentID, &count); err != nil {
		 return nil, fmt.Errorf("scan reviewer live assignment count: %w", err)
		}
		counts[agentID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reviewer live assignment counts: %w", err)
	}
	return counts, nil
}

func (s *Store) reviewerOpenSessionCollaborationCounts(ctx context.Context, workspaceID, generatorAgentID string, candidates []string) (map[string]int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	generatorAgentID = strings.TrimSpace(generatorAgentID)
	candidates = uniqueSortedStrings(candidates)
	if workspaceID == "" || generatorAgentID == "" || len(candidates) == 0 {
		return map[string]int{}, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(candidates)), ",")
	args := make([]any, 0, len(candidates)+2)
	args = append(args, workspaceID, generatorAgentID)
	for _, candidate := range candidates {
		args = append(args, candidate)
	}

	query := fmt.Sprintf(`SELECT assigned_to, COUNT(1)
		 FROM operator_queue_items
		 WHERE workspace_id = ?
		 AND status = 'OPEN'
		 AND source_kind = 'session_event'
		 AND agent_id = ?
		 AND queue_type IN ('DECISION', 'HANDOFF')
		 AND assigned_to IN (%s)
		 GROUP BY assigned_to`, placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query reviewer collaboration counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int, len(candidates))
	for rows.Next() {
		var assignedTo string
		var count int
		if err := rows.Scan(&assignedTo, &count); err != nil {
		 return nil, fmt.Errorf("scan reviewer collaboration count: %w", err)
		}
		assignedTo = strings.TrimSpace(assignedTo)
		if assignedTo == "" || count <= 0 {
		 continue
		}
		counts[assignedTo] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reviewer collaboration counts: %w", err)
	}
	return counts, nil
}

func orderReviewerCandidatesByTypedCollaborationAndLiveLoad(candidates []string, liveAssignmentCounts, collaborationCounts map[string]int, candidatePresence map[string]reviewerCandidatePresence) []string {
	if len(candidates) < 2 {
		return candidates
	}

	ordered := append([]string(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftPresence := candidatePresence[ordered[i]]
		rightPresence := candidatePresence[ordered[j]]
		if leftPresence.typedEligible != rightPresence.typedEligible {
		 return leftPresence.typedEligible
		}
		leftCollaboration := collaborationCounts[ordered[i]]
		rightCollaboration := collaborationCounts[ordered[j]]
		if leftCollaboration != rightCollaboration {
		 return leftCollaboration > rightCollaboration
		}
		left := liveAssignmentCounts[ordered[i]]
		right := liveAssignmentCounts[ordered[j]]
		if left != right {
		 return left < right
		}
		return false
	})
	return ordered
}

func (s *Store) reviewerActiveAssignmentCounts(ctx context.Context, workspaceID string) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT m.agent_id, COUNT(1)
		 FROM workspace_coalition_members m
		 JOIN workspace_coalitions c
		 ON c.workspace_id = m.workspace_id AND c.coalition_id = m.coalition_id
		 WHERE c.workspace_id = ?
		 AND c.status IN ('FORMING', 'ACTIVE')
		 AND m.role IN ('NEAR_REVIEWER', 'FAR_REVIEWER')
		 GROUP BY m.agent_id`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query active reviewer assignments: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var agentID string
		var count int
		if err := rows.Scan(&agentID, &count); err != nil {
		 return nil, fmt.Errorf("scan active reviewer assignment count: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" || count <= 0 {
		 continue
		}
		counts[agentID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active reviewer assignments: %w", err)
	}
	return counts, nil
}

func (s *Store) reviewerOpenSessionCollaborationLoad(ctx context.Context, workspaceID string, agentPresence map[string]reviewerCandidatePresence, typedCapacityOnly bool) (map[string]int, int, int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT assigned_to, COUNT(1)
		 FROM operator_queue_items
		 WHERE workspace_id = ?
		 AND status = 'OPEN'
		 AND source_kind = 'session_event'
		 AND queue_type IN ('DECISION', 'HANDOFF')
		 AND TRIM(assigned_to) != ''
		 GROUP BY assigned_to`,
		workspaceID,
	)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("query reviewer session collaboration load: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	generalistFallbackAssignments := 0
	ignoredNonReviewerAssignments := 0
	for rows.Next() {
		var assignedTo string
		var count int
		if err := rows.Scan(&assignedTo, &count); err != nil {
		 return nil, 0, 0, fmt.Errorf("scan reviewer session collaboration load: %w", err)
		}
		assignedTo = strings.TrimSpace(assignedTo)
		if assignedTo == "" || count <= 0 {
		 continue
		}
		presence, ok := agentPresence[assignedTo]
		if !ok || !presence.registered || !presence.online {
		 ignoredNonReviewerAssignments += count
		 continue
		}
		if typedCapacityOnly && !presence.typedEligible {
		 generalistFallbackAssignments += count
		 continue
		}
		counts[assignedTo] = count
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf("iterate reviewer session collaboration load: %w", err)
	}
	return counts, generalistFallbackAssignments, ignoredNonReviewerAssignments, nil
}

// RouteVerification dynamically evaluates a bundle against RRP-1.2 Verifier Mesh rules
// to assign the appropriate verification severity (Level 1-3) and select reviewers.
func (s *Store) RouteVerification(ctx context.Context, input VerifierMeshRouteInput) (VerifierMeshRoute, error) {
	route := VerifierMeshRoute{
		Advisory:      true,
		Level:         VerifierLevel1,
		Reasons:       make([]string, 0),
		IntegrityBand: "COMPLETE",
	}

	// Rule 1: Level 3 (Expensive) checks
	// Triggered if constraints mutated, active dissent exists, cluster stabilizing, or extreme risk.
	if input.TouchesHardConstraint {
		route.Level = VerifierLevel3
		route.Reasons = append(route.Reasons, "mutates_hard_constraint")
	} else if input.HasActiveDissent {
		route.Level = VerifierLevel3
		route.Reasons = append(route.Reasons, "active_dissent_present")
	} else if input.ClusterMode == "stabilize" {
		route.Level = VerifierLevel3
		route.Reasons = append(route.Reasons, "cluster_in_stabilize_mode")
	} else if input.MergeRisk > 0.85 {
		route.Level = VerifierLevel3
		route.Reasons = append(route.Reasons, "high_merge_risk")
	}

	// Rule 2: Level 2 (Medium) checks
	// Triggered if multi-patch bundle or moderate-to-high impact (but not Level 3).
	if route.Level == VerifierLevel1 {
		if input.IsMultiPatch {
		 route.Level = VerifierLevel2
		 route.Reasons = append(route.Reasons, "multi_patch_bundle")
		} else if input.ImpactScore > 0.60 {
		 route.Level = VerifierLevel2
		 route.Reasons = append(route.Reasons, "high_impact_score")
		} else if input.ContradictionPressure > 0.50 {
		 route.Level = VerifierLevel2
		 route.Reasons = append(route.Reasons, "elevated_contradiction_pressure")
		}
	}

	// Fallback reason if it stays at Level 1
	if route.Level == VerifierLevel1 {
		route.Reasons = append(route.Reasons, "default_fast_verification")
	}

	// Rule 3: Reviewer Assignment (Near/Far distribution)
	candidates := normalizeReviewerCandidates(input.GeneratorAgentID, input.AvailableReviewers)
	route.DedupedCandidateCount = len(candidates)
	if len(candidates) < len(input.AvailableReviewers) {
		route.Warnings = append(route.Warnings, "reviewer_candidates_deduped_or_generator_removed")
	}
	generatorRecord, err := s.GetAgent(ctx, input.WorkspaceID, input.GeneratorAgentID)
	switch {
	case err == nil && !generatorRecord.IsOnline:
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "generator_agent_offline")
		route.Warnings = append(route.Warnings, "generator_agent_offline")
	case err != nil && errors.Is(err, ErrAgentNotFound):
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "generator_agent_missing_workspace_registration")
		route.Warnings = append(route.Warnings, "generator_agent_missing_workspace_registration")
	case err != nil:
		return VerifierMeshRoute{}, err
	}
	evidenceCandidates, registeredCandidates, onlineCandidates, typedEligibleCandidates, candidatePresence, err := s.filterEvidenceBackedReviewerCandidates(ctx, input.WorkspaceID, candidates)
	if err != nil {
		return VerifierMeshRoute{}, err
	}
	route.RegisteredCandidates = registeredCandidates
	route.OnlineCandidates = onlineCandidates
	route.TypedEligibleCandidates = typedEligibleCandidates
	if registeredCandidates < len(candidates) {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_candidates_missing_workspace_registration")
		route.Warnings = append(route.Warnings, "reviewer_candidates_missing_workspace_registration")
	}
	if onlineCandidates < registeredCandidates {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_candidates_offline")
		route.Warnings = append(route.Warnings, "reviewer_candidates_offline")
	}
	if typedEligibleCandidates == 0 && onlineCandidates > 0 {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_candidates_missing_typed_eligibility")
		route.Warnings = append(route.Warnings, "reviewer_candidates_missing_typed_eligibility")
	} else if typedEligibleCandidates < onlineCandidates {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_pool_mixed_typed_eligibility")
		route.Warnings = append(route.Warnings, "reviewer_pool_mixed_typed_eligibility")
	}
	liveAssignmentCounts, err := s.reviewerLiveAssignmentCounts(ctx, input.WorkspaceID)
	if err != nil {
		return VerifierMeshRoute{}, err
	}
	collaborationCounts, err := s.reviewerOpenSessionCollaborationCounts(ctx, input.WorkspaceID, input.GeneratorAgentID, evidenceCandidates)
	if err != nil {
		return VerifierMeshRoute{}, err
	}
	for _, candidate := range evidenceCandidates {
		if collaborationCounts[candidate] > 0 {
		 route.ObservedCollaborationCandidates++
		}
	}
	if route.ObservedCollaborationCandidates == 0 && onlineCandidates > 0 {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_collaboration_unobserved")
		route.Warnings = append(route.Warnings, "reviewer_collaboration_unobserved")
	} else if route.ObservedCollaborationCandidates > 0 {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_collaboration_workspace_scoped")
		route.Warnings = append(route.Warnings, "reviewer_collaboration_workspace_scoped")
		if route.ObservedCollaborationCandidates < onlineCandidates {
		 route.IntegrityBand = "PARTIAL"
		 route.Reasons = append(route.Reasons, "reviewer_pool_mixed_collaboration_evidence")
		 route.Warnings = append(route.Warnings, "reviewer_pool_mixed_collaboration_evidence")
		}
	}
	evidenceCandidates = orderReviewerCandidatesByTypedCollaborationAndLiveLoad(evidenceCandidates, liveAssignmentCounts, collaborationCounts, candidatePresence)
	if len(evidenceCandidates) > 0 {
		heuristicCandidate := evidenceCandidates[0]
		heuristicObservedCollaboration := collaborationCounts[heuristicCandidate]
		if candidatePresence[heuristicCandidate].typedEligible {
		 route.NearReviewerEligibilityBasis = nearReviewerEligibilityTypedReviewer
		} else {
		 route.NearReviewerEligibilityBasis = nearReviewerEligibilityGeneralistFallback
		 route.NearReviewerFallbackReason = nearReviewerFallbackTypedEligibilityUnavailable
		}
		if heuristicObservedCollaboration > 0 {
		 route.NearReviewerStatus = nearReviewerStatusSelectedCollaborationEvidence
		 route.NearReviewer = heuristicCandidate
		 route.NearReviewerObservedCollaboration = heuristicObservedCollaboration
		 route.Reasons = append(route.Reasons, "near_reviewer_collaboration_workspace_scoped")
		 route.Warnings = append(route.Warnings, "near_reviewer_collaboration_workspace_scoped")
		 if candidatePresence[heuristicCandidate].typedEligible {
		 route.NearReviewerBasis = "typed_reviewer_eligible_observed_workspace_session_collaboration_then_live_reviewer_load_then_candidate_order"
		 } else {
		 route.NearReviewerBasis = "observed_workspace_session_collaboration_then_live_reviewer_load_then_candidate_order"
		 }
		 route.Reasons = append(route.Reasons, "near_reviewer_collaboration_observed")
		} else {
		 route.NearReviewerStatus = nearReviewerStatusOmittedHeuristicOnly
		 route.IntegrityBand = "PARTIAL"
		 route.Reasons = append(route.Reasons, "near_reviewer_collaboration_unobserved")
		 route.Reasons = append(route.Reasons, "near_reviewer_omitted_without_collaboration_evidence")
		 route.Warnings = append(route.Warnings, "near_reviewer_collaboration_unobserved")
		 if candidatePresence[heuristicCandidate].typedEligible {
		 route.NearReviewerBasis = "typed_reviewer_candidate_omitted_without_workspace_session_collaboration"
		 } else {
		 route.NearReviewerBasis = "generalist_candidate_omitted_without_workspace_session_collaboration"
		 }
		 if route.NearReviewerFallbackReason == "" {
		 route.NearReviewerFallbackReason = nearReviewerFallbackCollaborationUnobserved
		 }
		 route.Reasons = append(route.Reasons, "near_reviewer_heuristic_only")
		 route.Warnings = append(route.Warnings, "near_reviewer_heuristic_only")
		}
	}
	farCandidates := evidenceCandidates
	if route.NearReviewer != "" {
		farCandidates = make([]string, 0, len(evidenceCandidates)-1)
		for _, candidate := range evidenceCandidates {
		 if candidate == route.NearReviewer {
		 continue
		 }
		 farCandidates = append(farCandidates, candidate)
		}
	}
	if len(farCandidates) > 1 {
		bestCandidate := ""
		bestDistance := -1.0
		for _, candidate := range farCandidates {
		 distance, hasEvidence, err := s.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, input.WorkspaceID, input.GeneratorAgentID, candidate)
		 if err != nil || !hasEvidence || distance <= reviewerFarDistanceThreshold {
		 continue
		 }
		 if distance > bestDistance {
		 bestDistance = distance
		 bestCandidate = candidate
		 }
		}
		route.FarReviewer = bestCandidate
		if bestCandidate == "" {
		 route.Reasons = append(route.Reasons, "far_reviewer_evidence_unavailable")
		 route.FarReviewerStatus = farReviewerStatusOmittedNoDistanceEvidence
		 route.FarReviewerBasis = "no_evidence_backed_candidate"
		 route.IntegrityBand = "PARTIAL"
		 route.Warnings = append(route.Warnings, "far_reviewer_missing_evidence")
		} else {
		 route.Reasons = append(route.Reasons, "far_reviewer_evidence_backed")
		 route.FarReviewerStatus = farReviewerStatusSelectedDistanceEvidence
		 route.FarReviewerBasis = "pairwise_distance"
		}
	} else if len(candidates) == 1 || len(farCandidates) == 1 {
		route.FarReviewerStatus = farReviewerStatusOmittedInsufficientEvidenceCandidates
		route.FarReviewerBasis = "insufficient_evidence_backed_candidates"
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "far_reviewer_insufficient_evidence_backed_candidates")
		route.Warnings = append(route.Warnings, "far_reviewer_insufficient_evidence_backed_candidates")
	} else if len(farCandidates) == 0 {
		route.FarReviewerStatus = farReviewerStatusOmittedNoEvidenceCandidates
		route.FarReviewerBasis = "no_evidence_backed_candidate"
	}
	if route.NearReviewer == "" {
		if route.NearReviewerStatus == "" {
		 route.NearReviewerStatus = nearReviewerStatusOmittedNoEvidenceCandidates
		}
		route.Reasons = append(route.Reasons, "near_reviewer_unavailable")
		if route.NearReviewerStatus == nearReviewerStatusOmittedNoEvidenceCandidates {
		 route.NearReviewerEligibilityBasis = nearReviewerEligibilityNone
		 route.NearReviewerFallbackReason = nearReviewerFallbackNoWorkspaceOnlineCandidate
		 route.NearReviewerBasis = "no_workspace_online_candidate"
		}
		route.IntegrityBand = "PARTIAL"
	}

	scarcity, err := s.ReviewerMeshScarcitySnapshot(ctx, input.WorkspaceID)
	if err != nil {
		return VerifierMeshRoute{}, err
	}
	if strings.EqualFold(strings.TrimSpace(scarcity.Status), "SATURATED") {
		route.IntegrityBand = "PARTIAL"
		route.Reasons = append(route.Reasons, "reviewer_scarcity_saturated")
		route.Warnings = append(route.Warnings, "reviewer_scarcity_saturated")
		route.NearReviewer = ""
		route.NearReviewerObservedCollaboration = 0
		route.NearReviewerStatus = nearReviewerStatusOmittedScarcitySaturated
		route.NearReviewerBasis = "saturated_reviewer_mesh"
		route.NearReviewerFallbackReason = nearReviewerFallbackScarcitySaturated
		route.FarReviewer = ""
		route.FarReviewerStatus = farReviewerStatusOmittedScarcitySaturated
		route.FarReviewerBasis = "saturated_reviewer_mesh"
	}

	return route, nil
}

func (s *Store) ReviewerMeshScarcitySnapshot(ctx context.Context, workspaceID string) (ReviewerMeshScarcitySnapshot, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return ReviewerMeshScarcitySnapshot{}, fmt.Errorf("workspace_id is required")
	}

	snapshot := ReviewerMeshScarcitySnapshot{
		WorkspaceID:    workspaceID,
		Status:         "UNKNOWN",
		IntegrityBand:  "PARTIAL",
		EvidenceSource: "typed_reviewer_agents_plus_live_coalition_roles_plus_session_collaboration_queue",
		Reasons:        []string{},
	}

	agentPresence, registeredAgents, onlineAgents, registeredTypedReviewers, onlineTypedReviewers, err := s.reviewerAgentPresence(ctx, workspaceID)
	if err != nil {
		return ReviewerMeshScarcitySnapshot{}, err
	}
	snapshot.RegisteredAgents = registeredAgents
	snapshot.OnlineAgents = onlineAgents
	snapshot.RegisteredTypedReviewers = registeredTypedReviewers
	snapshot.OnlineTypedReviewers = onlineTypedReviewers
	if snapshot.OnlineTypedReviewers > 0 {
		snapshot.CapacityBasis = reviewerCapacityBasisOnlineTypedReviewerUpperBound
		snapshot.CapacityUpperBound = snapshot.OnlineTypedReviewers
		snapshot.Reasons = append(snapshot.Reasons, "capacity_uses_online_typed_reviewer_upper_bound")
		if snapshot.OnlineTypedReviewers < snapshot.OnlineAgents {
		 snapshot.Reasons = append(snapshot.Reasons, "generalist_online_agents_excluded_from_typed_capacity")
		}
	} else {
		snapshot.CapacityBasis = reviewerCapacityBasisOnlineAgentUpperBoundFallback
		snapshot.CapacityUpperBound = snapshot.OnlineAgents
		snapshot.Reasons = append(snapshot.Reasons, "no_online_typed_reviewers", "capacity_falls_back_to_online_agent_upper_bound")
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM workspace_coalitions
		 WHERE workspace_id = ? AND status IN ('FORMING', 'ACTIVE')`,
		workspaceID,
	).Scan(&snapshot.LiveCoalitions); err != nil {
		return ReviewerMeshScarcitySnapshot{}, fmt.Errorf("count live coalitions: %w", err)
	}

	reviewerAssignmentCounts, err := s.reviewerActiveAssignmentCounts(ctx, workspaceID)
	if err != nil {
		return ReviewerMeshScarcitySnapshot{}, err
	}
	for agentID, count := range reviewerAssignmentCounts {
		snapshot.ActiveReviewerAssignments += count
		if agentPresence[agentID].typedEligible {
		 snapshot.ActiveTypedReviewerAssignments += count
		} else {
		 snapshot.ActiveGeneralistFallbackAssignments += count
		}
	}
	snapshot.DistinctActiveReviewers = len(reviewerAssignmentCounts)
	snapshot.ReviewerLoadHHI = instrumentationHHIFromCounts(reviewerAssignmentCounts)
	if snapshot.ActiveGeneralistFallbackAssignments > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "generalist_fallback_reviewer_assignments_observed")
	}
	sessionCollaborationCounts, generalistFallbackSessionAssignments, ignoredNonReviewerSessionAssignments, err := s.reviewerOpenSessionCollaborationLoad(ctx, workspaceID, agentPresence, snapshot.OnlineTypedReviewers > 0)
	if err != nil {
		return ReviewerMeshScarcitySnapshot{}, err
	}
	for _, count := range sessionCollaborationCounts {
		snapshot.OpenSessionCollaborationAssignments += count
	}
	snapshot.DistinctSessionCollaborationReviewers = len(sessionCollaborationCounts)
	snapshot.SessionCollaborationLoadHHI = instrumentationHHIFromCounts(sessionCollaborationCounts)
	if generalistFallbackSessionAssignments > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "generalist_session_collaboration_assignments_ignored_for_typed_capacity")
	}
	if ignoredNonReviewerSessionAssignments > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "non_capacity_session_collaboration_assignments_ignored")
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		 FROM (
		 SELECT c.coalition_id,
		 COUNT(m.agent_id) AS member_count,
		 SUM(CASE WHEN m.role = 'FAR_REVIEWER' THEN 1 ELSE 0 END) AS far_count
		 FROM workspace_coalitions c
		 LEFT JOIN workspace_coalition_members m
		 ON m.workspace_id = c.workspace_id AND m.coalition_id = c.coalition_id
		 WHERE c.workspace_id = ?
		 AND c.status IN ('FORMING', 'ACTIVE')
		 GROUP BY c.coalition_id
		 ) live
		 WHERE member_count >= 2 AND far_count = 0`,
		workspaceID,
	).Scan(&snapshot.CoalitionsMissingFarReview); err != nil {
		return ReviewerMeshScarcitySnapshot{}, fmt.Errorf("count live coalitions missing far reviewer: %w", err)
	}

	switch {
	case snapshot.RegisteredAgents == 0:
		snapshot.IntegrityBand = "UNKNOWN"
		snapshot.Reasons = append(snapshot.Reasons, "no_registered_agents")
	case snapshot.OnlineAgents == 0:
		snapshot.IntegrityBand = "UNKNOWN"
		snapshot.Reasons = append(snapshot.Reasons, "no_online_agents")
	case snapshot.CapacityUpperBound == 0:
		snapshot.Status = "UNKNOWN"
		snapshot.IntegrityBand = "UNKNOWN"
		snapshot.Reasons = append(snapshot.Reasons, "no_reviewer_capacity_evidence")
	default:
		effectiveDistinctActiveReviewers := snapshot.DistinctActiveReviewers
		effectiveActiveReviewerAssignments := snapshot.ActiveReviewerAssignments
		effectiveReviewerLoadHHI := snapshot.ReviewerLoadHHI
		if snapshot.CapacityBasis == reviewerCapacityBasisOnlineTypedReviewerUpperBound {
		 typedReviewerAssignmentCounts := make(map[string]int)
		 for agentID, count := range reviewerAssignmentCounts {
		 if !agentPresence[agentID].typedEligible {
		 continue
		 }
		 typedReviewerAssignmentCounts[agentID] = count
		 }
		 effectiveDistinctActiveReviewers = len(typedReviewerAssignmentCounts)
		 effectiveActiveReviewerAssignments = snapshot.ActiveTypedReviewerAssignments
		 effectiveReviewerLoadHHI = instrumentationHHIFromCounts(typedReviewerAssignmentCounts)
		 if snapshot.ActiveGeneralistFallbackAssignments > 0 {
		 snapshot.Reasons = append(snapshot.Reasons, "typed_capacity_excludes_generalist_fallback_assignments")
		 }
		}
		snapshot.AvailableHeadroom = snapshot.CapacityUpperBound - effectiveDistinctActiveReviewers
		if snapshot.AvailableHeadroom < 0 {
		 snapshot.AvailableHeadroom = 0
		}
		if effectiveActiveReviewerAssignments == 0 || effectiveDistinctActiveReviewers == 0 {
		 snapshot.Status = "UNKNOWN"
		 snapshot.Reasons = append(snapshot.Reasons, "no_active_reviewer_load")
		 if snapshot.LiveCoalitions > 0 {
		 snapshot.Reasons = append(snapshot.Reasons, "live_coalitions_without_reviewer_assignments")
		 }
		 break
		}
		utilization := float64(effectiveDistinctActiveReviewers) / float64(snapshot.CapacityUpperBound)
		switch {
		case effectiveDistinctActiveReviewers >= snapshot.CapacityUpperBound:
		 snapshot.Status = "SATURATED"
		case effectiveActiveReviewerAssignments > 1 && effectiveReviewerLoadHHI >= reviewerLoadConcentrationScarceThreshold:
		 snapshot.Status = "SCARCE"
		 snapshot.Reasons = append(snapshot.Reasons, "reviewer_load_highly_concentrated")
		case utilization >= 0.75:
		 snapshot.Status = "SCARCE"
		default:
		 snapshot.Status = "UNKNOWN"
		 snapshot.Reasons = append(snapshot.Reasons, "capacity_upper_bound_only_low_utilization")
		}
	}
	if snapshot.Status != "SATURATED" && snapshot.ActiveGeneralistFallbackAssignments > 0 && snapshot.OnlineTypedReviewers > 0 {
		snapshot.Status = "SCARCE"
	}
	if snapshot.OpenSessionCollaborationAssignments == 0 {
		snapshot.Reasons = append(snapshot.Reasons, "session_collaboration_load_unobserved")
	} else {
		snapshot.Reasons = append(snapshot.Reasons, "session_collaboration_load_workspace_scoped")
		if snapshot.OpenSessionCollaborationAssignments > 1 && snapshot.SessionCollaborationLoadHHI >= reviewerLoadConcentrationScarceThreshold {
		 if snapshot.Status != "SATURATED" {
		 snapshot.Status = "SCARCE"
		 }
		 snapshot.Reasons = append(snapshot.Reasons, "session_collaboration_load_highly_concentrated")
		}
	}
	if snapshot.Status == "SCARCE" && snapshot.OpenSessionCollaborationAssignments == 0 {
		snapshot.Reasons = append(snapshot.Reasons, "scarcity_driven_by_role_load_without_session_collaboration_evidence")
	}
	if snapshot.CoalitionsMissingFarReview > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "far_reviewer_gaps_observed")
	}

	return snapshot, nil
}
