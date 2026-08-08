package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	rspBeliefSignalType              = "BELIEF_UPDATE"
	rspBeliefShadowPhase             = "S0"
	rspBeliefLowIndependenceCutoff   = 0.70
	rspBeliefHighContradictionCutoff = 0.45
	rspBeliefHighUncertaintyCutoff   = 0.50
)

type RSPBeliefReportFilter struct {
	WorkspaceID string
	ClaimType   string
	AgentID     string
	SessionID   string
	TaskID      string
	Limit       int
}

type RSPBeliefClaimReport struct {
	WorkspaceID           string                 `json:"workspace_id"`
	TimeAuthority         WorkspaceTimeAuthority `json:"time_authority"`
	ClaimID               string                 `json:"claim_id"`
	ClaimType             string                 `json:"claim_type"`
	BeliefDomain          string                 `json:"belief_domain"`
	Status                string                 `json:"status"`
	Subject               string                 `json:"subject"`
	ClaimUpdatedAt        string                 `json:"claim_updated_at"`
	SignalType            string                 `json:"signal_type"`
	ShadowMode            bool                   `json:"shadow_mode"`
	ShadowPhase           string                 `json:"shadow_phase"`
	Posterior             float64                `json:"posterior"`
	Uncertainty           float64                `json:"uncertainty"`
	EvidenceMass          float64                `json:"evidence_mass"`
	EvidenceUnitCount     int                    `json:"evidence_unit_count"`
	EvidenceDiversity     float64                `json:"evidence_diversity"`
	SourceDiversity       float64                `json:"source_diversity"`
	IndependenceDiscount  float64                `json:"independence_discount"`
	IndependentGroups     int                    `json:"independent_evidence_groups"`
	CorrelatedEvidence    int                    `json:"correlated_evidence_count"`
	SupportMass           float64                `json:"support_mass"`
	ContradictionMass     float64                `json:"contradiction_mass"`
	ContradictionPressure float64                `json:"contradiction_pressure"`
	DriftScore            float64                `json:"drift_score"`
	DriftState            string                 `json:"drift_state,omitempty"`
	VerifierFresh         bool                   `json:"verifier_fresh"`
	SuggestedState        string                 `json:"suggested_state"`
	IndependenceBasis     string                 `json:"independence_basis,omitempty"`
	RootCauseGroups       []string               `json:"root_cause_groups,omitempty"`
	EvidenceRefs          []string               `json:"evidence_refs,omitempty"`
	Summary               string                 `json:"summary"`
}

type RSPBeliefReport struct {
	WorkspaceID            string                 `json:"workspace_id"`
	TimeAuthority          WorkspaceTimeAuthority `json:"time_authority"`
	ClaimType              string                 `json:"claim_type,omitempty"`
	AgentID                string                 `json:"agent_id,omitempty"`
	SessionID              string                 `json:"session_id,omitempty"`
	TaskID                 string                 `json:"task_id,omitempty"`
	SignalType             string                 `json:"signal_type"`
	ShadowPhase            string                 `json:"shadow_phase"`
	Calibration            RSPCalibrationContract `json:"calibration"`
	Items                  []RSPBeliefClaimReport `json:"items"`
	Count                  int                    `json:"count"`
	StableCount            int                    `json:"stable_count"`
	NeedsReviewCount       int                    `json:"needs_review_count"`
	HighDriftCount         int                    `json:"high_drift_count"`
	LowIndependenceCount   int                    `json:"low_independence_count"`
	HighContradictionCount int                    `json:"high_contradiction_count"`
	VerifierStaleCount     int                    `json:"verifier_stale_count"`
	HighUncertaintyCount   int                    `json:"high_uncertainty_count"`
	CapabilityFlags        RSPCapabilityFlags     `json:"capability_flags"`
	GeneratedAt            string                 `json:"generated_at"`
	Summary                string                 `json:"summary"`
}

type RSPBeliefSnapshotResult struct {
	Report RSPBeliefReport    `json:"report"`
	Event  RuntimeEventRecord `json:"event"`
}

func (s *Store) BuildRSPBeliefReport(ctx context.Context, filter RSPBeliefReportFilter) (RSPBeliefReport, error) {
	filter, err := normalizeRSPBeliefReportFilter(filter)
	if err != nil {
		return RSPBeliefReport{}, err
	}
	claims, err := s.listRSPBeliefClaims(ctx, filter)
	if err != nil {
		return RSPBeliefReport{}, err
	}

	report := RSPBeliefReport{
		WorkspaceID:     filter.WorkspaceID,
		ClaimType:       filter.ClaimType,
		AgentID:         filter.AgentID,
		SessionID:       filter.SessionID,
		TaskID:          filter.TaskID,
		SignalType:      rspBeliefSignalType,
		ShadowPhase:     rspBeliefShadowPhase,
		Calibration:     rspBeliefReadModelCalibrationContract(),
		CapabilityFlags: s.GetRSPCapabilityFlags(ctx, filter.WorkspaceID),
		Items:           make([]RSPBeliefClaimReport, 0, len(claims)),
	}
	report.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return RSPBeliefReport{}, err
	}
	report.GeneratedAt = generatedAtFromWorkspaceTimeAuthority(report.TimeAuthority)
	for _, claim := range claims {
		item, err := s.buildRSPBeliefClaimReport(ctx, claim)
		if err != nil {
			return RSPBeliefReport{}, err
		}
		item.TimeAuthority = report.TimeAuthority
		report.Items = append(report.Items, item)
		if rspBeliefSuggestedStateStable(item.SuggestedState) {
			report.StableCount++
		} else {
			report.NeedsReviewCount++
		}
		if item.DriftScore >= 0.35 {
			report.HighDriftCount++
		}
		if item.IndependenceDiscount < rspBeliefLowIndependenceCutoff {
			report.LowIndependenceCount++
		}
		if item.ContradictionPressure >= rspBeliefHighContradictionCutoff {
			report.HighContradictionCount++
		}
		if !item.VerifierFresh {
			report.VerifierStaleCount++
		}
		if item.Uncertainty >= rspBeliefHighUncertaintyCutoff {
			report.HighUncertaintyCount++
		}
	}
	sort.Slice(report.Items, func(i, j int) bool {
		return rspBeliefReportLess(report.Items[i], report.Items[j])
	})
	if len(report.Items) > filter.Limit {
		report.Items = report.Items[:filter.Limit]
	}
	report.Count = len(report.Items)
	report.Summary = fmt.Sprintf(
		"rsp belief shadow report for %s: %d items, %d stable, %d needing review, %d low independence, %d high contradiction, %d verifier stale, %d high uncertainty",
		report.WorkspaceID,
		report.Count,
		report.StableCount,
		report.NeedsReviewCount,
		report.LowIndependenceCount,
		report.HighContradictionCount,
		report.VerifierStaleCount,
		report.HighUncertaintyCount,
	)
	return report, nil
}

func (s *Store) GetRSPBeliefClaim(ctx context.Context, workspaceID, claimID string) (RSPBeliefClaimReport, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	claimID = strings.TrimSpace(claimID)
	if workspaceID == "" {
		return RSPBeliefClaimReport{}, errors.New("workspace_id is required")
	}
	if claimID == "" {
		return RSPBeliefClaimReport{}, errors.New("claim_id is required")
	}
	record, err := s.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}
	if rspBeliefDomainForClaimType(record.ClaimType) == "" {
		return RSPBeliefClaimReport{}, fmt.Errorf("claim_type %q is not supported by rsp stage-1", strings.TrimSpace(record.ClaimType))
	}
	item, err := s.buildRSPBeliefClaimReport(ctx, record)
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}
	item.TimeAuthority, err = s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}
	return item, nil
}

func (s *Store) SnapshotRSPBeliefReport(ctx context.Context, filter RSPBeliefReportFilter) (RSPBeliefSnapshotResult, error) {
	if err := s.ensureRSPCapabilityEnabled(ctx, filter.WorkspaceID, rspCapabilityBeliefLive); err != nil {
		return RSPBeliefSnapshotResult{}, err
	}
	report, err := s.BuildRSPBeliefReport(ctx, filter)
	if err != nil {
		return RSPBeliefSnapshotResult{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, report.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RSPBeliefSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RSPBeliefSnapshotResult{}, fmt.Errorf("begin rsp belief snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	event := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		event, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: report.WorkspaceID,
			EventType:   "rsp.belief_snapshot",
			EntityType:  "rsp_belief",
			EntityID:    rspBeliefSnapshotEntityID(report),
			ActorType:   "system",
			ActorID:     "rsp_belief",
			PayloadJSON: mustJSON(rspBeliefSnapshotPayload(report)),
			CreatedAt:   now,
		})
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return RSPBeliefSnapshotResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RSPBeliefSnapshotResult{}, fmt.Errorf("commit rsp belief snapshot tx: %w", err)
	}
	return RSPBeliefSnapshotResult{Report: report, Event: event}, nil
}

func (s *Store) buildRSPBeliefUpdatesForClaims(ctx context.Context, claims []KnowledgeClaimRecord) []RSPBeliefClaimReport {
	if len(claims) == 0 {
		return nil
	}
	updates := make([]RSPBeliefClaimReport, 0, len(claims))
	var authority WorkspaceTimeAuthority
	if workspaceID := strings.TrimSpace(claims[0].WorkspaceID); workspaceID != "" {
		if itemAuthority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID); err == nil {
			authority = itemAuthority
		}
	}
	for _, claim := range claims {
		if rspBeliefDomainForClaimType(claim.ClaimType) == "" {
			continue
		}
		item, err := s.buildRSPBeliefClaimReport(ctx, claim)
		if err != nil {
			continue
		}
		item.TimeAuthority = authority
		updates = append(updates, item)
	}
	sort.Slice(updates, func(i, j int) bool {
		return rspBeliefReportLess(updates[i], updates[j])
	})
	return updates
}

func (s *Store) listRSPBeliefClaims(ctx context.Context, filter RSPBeliefReportFilter) ([]KnowledgeClaimRecord, error) {
	outByID := make(map[string]KnowledgeClaimRecord)
	for _, claimType := range rspBeliefFilterClaimTypes(filter.ClaimType) {
		items, err := s.ListKnowledgeClaims(ctx, KnowledgeClaimFilter{
			WorkspaceID: filter.WorkspaceID,
			ClaimType:   claimType,
			AgentID:     filter.AgentID,
			SessionID:   filter.SessionID,
			TaskID:      filter.TaskID,
			Limit:       filter.Limit,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if rspBeliefDomainForClaimType(item.ClaimType) == "" {
				continue
			}
			outByID[item.ClaimID] = item
		}
	}
	out := make([]KnowledgeClaimRecord, 0, len(outByID))
	for _, item := range outByID {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if rspBeliefClaimStatusPriority(left.Status) != rspBeliefClaimStatusPriority(right.Status) {
			return rspBeliefClaimStatusPriority(left.Status) > rspBeliefClaimStatusPriority(right.Status)
		}
		if left.UpdatedAt != right.UpdatedAt {
			return left.UpdatedAt > right.UpdatedAt
		}
		if left.Confidence != right.Confidence {
			return left.Confidence > right.Confidence
		}
		return left.ClaimID > right.ClaimID
	})
	if len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *Store) buildRSPBeliefClaimReport(ctx context.Context, claim KnowledgeClaimRecord) (RSPBeliefClaimReport, error) {
	domain := rspBeliefDomainForClaimType(claim.ClaimType)
	if domain == "" {
		return RSPBeliefClaimReport{}, fmt.Errorf("claim_type %q is not supported by rsp stage-1", strings.TrimSpace(claim.ClaimType))
	}

	relations, err := s.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: claim.WorkspaceID,
		ClaimID:     claim.ClaimID,
		Limit:       200,
	})
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}
	driftScore, driftState, comparedRefCount, err := s.loadRSPBeliefDrift(ctx, claim.WorkspaceID, claim.ClaimID)
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}
	peerClaims, err := s.loadRSPBeliefPeerClaims(ctx, claim.WorkspaceID, claim.ClaimID, relations)
	if err != nil {
		return RSPBeliefClaimReport{}, err
	}

	prior := rspBeliefPrior(claim)
	statusSupport, statusChallenge, statusMass := rspBeliefStatusContribution(claim.Status)
	supportMass := 0.0
	challengeMass := 0.0
	repairMass := 0.0
	evidenceUnitCount := 0
	nonRelationEvidenceCount := 0
	independentGroups := map[string]struct{}{}
	sourceGroups := map[string]struct{}{}
	unrootedIndependentGroups := map[string]struct{}{}
	unrootedSourceGroups := map[string]struct{}{}
	rootCauseGroups := map[string]struct{}{}
	rootCauseAnchoredUnits := 0
	evidenceRefs := make([]string, 0, len(relations)+len(claim.Evidence)+1)
	claimGroup := rspBeliefClaimGroupKey(claim)
	claimSourceGroup := rspBeliefClaimSourceGroupKey(claim)

	if claimGroup != "" {
		independentGroups[claimGroup] = struct{}{}
	}
	if claimSourceGroup != "" {
		sourceGroups[claimSourceGroup] = struct{}{}
	}
	if claimRootCauseGroups := s.rspBeliefClaimRootCauseGroups(ctx, claim); len(claimRootCauseGroups) > 0 {
		rootCauseAnchoredUnits++
		for _, rootCauseGroup := range claimRootCauseGroups {
			rootCauseGroups[rootCauseGroup] = struct{}{}
		}
	} else {
		if claimGroup != "" {
			unrootedIndependentGroups[claimGroup] = struct{}{}
		}
		if claimSourceGroup != "" {
			unrootedSourceGroups[claimSourceGroup] = struct{}{}
		}
	}
	if claimGroup != "" || claimSourceGroup != "" {
		evidenceUnitCount++
	}
	for _, evidence := range claim.Evidence {
		evidence = strings.TrimSpace(evidence)
		if evidence == "" {
			continue
		}
		evidenceRefs = append(evidenceRefs, evidence)
		if _, _, _, structured := parseKnowledgeClaimRelationEvidence(evidence); structured {
			continue
		}
		evidenceUnitCount++
		nonRelationEvidenceCount++
		if group := rspBeliefEvidenceGroupKey(evidence); group != "" {
			independentGroups[group] = struct{}{}
			sourceGroups[group] = struct{}{}
		}
		if evidenceRootCauseGroups := s.rspBeliefEvidenceRootCauseGroups(ctx, claim.WorkspaceID, evidence); len(evidenceRootCauseGroups) > 0 {
			rootCauseAnchoredUnits++
			for _, rootCauseGroup := range evidenceRootCauseGroups {
				rootCauseGroups[rootCauseGroup] = struct{}{}
			}
		} else {
			if group := rspBeliefEvidenceGroupKey(evidence); group != "" {
				unrootedIndependentGroups[group] = struct{}{}
				unrootedSourceGroups[group] = struct{}{}
			}
		}
	}

	verifierFresh := normalizeKnowledgeClaimStatus(claim.Status) == "CONFIRMED"
	supersededRisk := normalizeKnowledgeClaimStatus(claim.Status) == "SUPERSEDED"
	for _, relation := range relations {
		evidenceUnitCount++
		evidenceRefs = append(evidenceRefs, relation.RelationID)
		if group := rspBeliefRelationGroupKey(relation, claim.ClaimID, peerClaims); group != "" {
			independentGroups[group] = struct{}{}
		}
		if sourceGroup := rspBeliefRelationSourceGroupKey(relation, claim.ClaimID, peerClaims); sourceGroup != "" {
			sourceGroups[sourceGroup] = struct{}{}
		}
		if relationRootCauseGroups := s.rspBeliefRelationRootCauseGroups(ctx, claim.WorkspaceID, relation, claim.ClaimID, peerClaims); len(relationRootCauseGroups) > 0 {
			rootCauseAnchoredUnits++
			for _, rootCauseGroup := range relationRootCauseGroups {
				rootCauseGroups[rootCauseGroup] = struct{}{}
			}
		} else {
			if group := rspBeliefRelationGroupKey(relation, claim.ClaimID, peerClaims); group != "" {
				unrootedIndependentGroups[group] = struct{}{}
			}
			if sourceGroup := rspBeliefRelationSourceGroupKey(relation, claim.ClaimID, peerClaims); sourceGroup != "" {
				unrootedSourceGroups[sourceGroup] = struct{}{}
			}
		}
		relationSupport, relationChallenge, relationRepair, relationVerifierFresh, relationSupersededRisk := rspBeliefRelationContribution(domain, claim.ClaimID, relation)
		supportMass += relationSupport
		challengeMass += relationChallenge
		repairMass += relationRepair
		verifierFresh = verifierFresh || relationVerifierFresh
		supersededRisk = supersededRisk || relationSupersededRisk
	}

	effectiveIndependentGroups := len(independentGroups)
	effectiveSourceGroups := len(sourceGroups)
	rootCauseGroupList := uniqueTrimmedLocusStrings(mapKeys(rootCauseGroups))
	if len(rootCauseGroupList) > 0 {
		effectiveIndependentGroups = len(rootCauseGroupList) + len(unrootedIndependentGroups)
		effectiveSourceGroups = len(rootCauseGroupList) + len(unrootedSourceGroups)
	}

	evidenceDiversity := 0.0
	if evidenceUnitCount > 0 {
		evidenceDiversity = math.Sqrt(float64(effectiveIndependentGroups) / float64(evidenceUnitCount))
		if evidenceDiversity > 1 {
			evidenceDiversity = 1
		}
	}
	sourceDiversity := 0.0
	if evidenceUnitCount > 0 {
		sourceDiversity = math.Sqrt(float64(effectiveSourceGroups) / float64(evidenceUnitCount))
		if sourceDiversity > 1 {
			sourceDiversity = 1
		}
	}
	independenceDiscount := 1.0
	if evidenceUnitCount > 0 {
		independenceDiscount = rspBeliefClamp(0.30, math.Min(evidenceDiversity, sourceDiversity), 1)
	}
	supportMass *= independenceDiscount
	challengeMass *= independenceDiscount
	repairMass *= independenceDiscount

	totalSupportMass := statusSupport + supportMass
	totalChallengeMass := statusChallenge + challengeMass + repairMass
	driftPenalty := clampUnitInterval(driftScore) * 1.1
	posterior := rspBeliefSigmoid(rspBeliefLogit(prior) + totalSupportMass - totalChallengeMass - driftPenalty)
	evidenceMass := statusMass + totalSupportMass + totalChallengeMass + float64(nonRelationEvidenceCount)*0.2
	if verifierFresh {
		evidenceMass += 0.3
	}
	if comparedRefCount == 0 {
		evidenceMass *= 0.85
	}
	uncertainty := 1 / math.Sqrt(1+math.Max(0, evidenceMass))
	if evidenceUnitCount == 0 {
		uncertainty = math.Max(uncertainty, 0.6)
	}
	if driftState == "" {
		uncertainty = clampUnitInterval(uncertainty + 0.08)
	}
	contradictionPressure := 0.0
	if totalSupportMass+totalChallengeMass > 0 {
		contradictionPressure = totalChallengeMass / (totalSupportMass + totalChallengeMass)
	}
	suggestedState := rspBeliefSuggestedState(domain, claim.Status, posterior, uncertainty, driftScore, contradictionPressure, verifierFresh, supersededRisk, repairMass > supportMass*0.5)

	independenceBasis := "HEURISTIC_GROUPS"
	switch {
	case len(rootCauseGroupList) > 0 && rootCauseAnchoredUnits >= evidenceUnitCount:
		independenceBasis = "ROOT_CAUSE_ANCHORED"
	case len(rootCauseGroupList) > 0:
		independenceBasis = "ROOT_CAUSE_MIXED"
	}

	item := RSPBeliefClaimReport{
		WorkspaceID:           strings.TrimSpace(claim.WorkspaceID),
		ClaimID:               strings.TrimSpace(claim.ClaimID),
		ClaimType:             strings.TrimSpace(claim.ClaimType),
		BeliefDomain:          domain,
		Status:                normalizeKnowledgeClaimStatus(claim.Status),
		Subject:               firstNonEmpty(strings.TrimSpace(claim.Subject), strings.TrimSpace(claim.Summary)),
		ClaimUpdatedAt:        firstNonEmpty(strings.TrimSpace(claim.UpdatedAt), strings.TrimSpace(claim.CreatedAt)),
		SignalType:            rspBeliefSignalType,
		ShadowMode:            true,
		ShadowPhase:           rspBeliefShadowPhase,
		Posterior:             clampUnitInterval(posterior),
		Uncertainty:           clampUnitInterval(uncertainty),
		EvidenceMass:          evidenceMass,
		EvidenceUnitCount:     evidenceUnitCount,
		EvidenceDiversity:     evidenceDiversity,
		SourceDiversity:       sourceDiversity,
		IndependenceDiscount:  independenceDiscount,
		IndependentGroups:     effectiveIndependentGroups,
		CorrelatedEvidence:    maxInt(0, evidenceUnitCount-effectiveIndependentGroups),
		SupportMass:           totalSupportMass,
		ContradictionMass:     totalChallengeMass,
		ContradictionPressure: clampUnitInterval(contradictionPressure),
		DriftScore:            clampUnitInterval(driftScore),
		DriftState:            firstNonEmpty(strings.TrimSpace(driftState), "UNTRACKED"),
		VerifierFresh:         verifierFresh,
		SuggestedState:        suggestedState,
		IndependenceBasis:     independenceBasis,
		RootCauseGroups:       rootCauseGroupList,
		EvidenceRefs:          rspBeliefEvidenceRefs(evidenceRefs),
	}
	summaryParts := []string{
		fmt.Sprintf(
			"%s belief posterior %.2f with %s uncertainty %.2f, drift %.2f, suggested state %s",
			strings.ToLower(item.BeliefDomain),
			item.Posterior,
			strings.ToLower(item.DriftState),
			item.Uncertainty,
			item.DriftScore,
			strings.ToLower(strings.ReplaceAll(item.SuggestedState, "_", " ")),
		),
	}
	if item.IndependenceBasis != "" && item.IndependenceBasis != "HEURISTIC_GROUPS" {
		summaryParts = append(summaryParts, "independence "+strings.ToLower(strings.ReplaceAll(item.IndependenceBasis, "_", " ")))
	}
	if len(item.RootCauseGroups) > 0 {
		summaryParts = append(summaryParts, "root causes "+strings.Join(item.RootCauseGroups, ", "))
	}
	item.Summary = strings.Join(summaryParts, " | ")
	return item, nil
}

func (s *Store) loadRSPBeliefDrift(ctx context.Context, workspaceID, claimID string) (float64, string, int, error) {
	detail, err := s.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", claimID))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return 0, "", 0, nil
		}
		return 0, "", 0, err
	}
	if detail.DriftReport == nil {
		return detail.Node.Drift, "", 0, nil
	}
	return detail.DriftReport.Drift, strings.TrimSpace(detail.DriftReport.Status), detail.DriftReport.ComparedRefCount, nil
}

func normalizeRSPBeliefReportFilter(filter RSPBeliefReportFilter) (RSPBeliefReportFilter, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.ClaimType = strings.TrimSpace(filter.ClaimType)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	if filter.WorkspaceID == "" {
		return RSPBeliefReportFilter{}, errors.New("workspace_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	if filter.ClaimType != "" && rspBeliefDomainForClaimType(filter.ClaimType) == "" {
		return RSPBeliefReportFilter{}, fmt.Errorf("claim_type %q is not supported by rsp stage-1", filter.ClaimType)
	}
	return filter, nil
}

func rspBeliefFilterClaimTypes(claimType string) []string {
	claimType = strings.TrimSpace(claimType)
	if claimType != "" {
		switch strings.ToUpper(claimType) {
		case "DECISION", "DECISION_RECORD":
			return []string{"DECISION", "DECISION_RECORD"}
		case "BLOCKER":
			return []string{"BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS"}
		case "BLOCKER_SYMPTOM":
			return []string{"BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM"}
		case "BLOCKER_HYPOTHESIS":
			return []string{"BLOCKER_HYPOTHESIS"}
		default:
			return []string{normalizeKnowledgeClaimType(claimType)}
		}
	}
	return []string{"FACT", "ENTITY", "LESSON", "CONSTRAINT", "DECISION", "DECISION_RECORD", "BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS"}
}

func rspBeliefDomainForClaimType(claimType string) string {
	switch strings.ToUpper(strings.TrimSpace(claimType)) {
	case "FACT", "ENTITY", "LESSON", "CONSTRAINT":
		return "FACT"
	case "DECISION", "DECISION_RECORD":
		return "DECISION"
	case "BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS":
		return "BLOCKER"
	default:
		return ""
	}
}

func rspBeliefPrior(claim KnowledgeClaimRecord) float64 {
	confidence := clampUnitInterval(claim.Confidence)
	statusPrior := 0.55
	switch normalizeKnowledgeClaimStatus(claim.Status) {
	case "CONFIRMED":
		statusPrior = 0.82
	case "ACTIVE":
		statusPrior = 0.62
	case "REVIEW":
		statusPrior = 0.52
	case "STALE":
		statusPrior = 0.35
	case "DISPUTED":
		statusPrior = 0.22
	case "SUPERSEDED":
		statusPrior = 0.18
	}
	if confidence == 0 {
		return statusPrior
	}
	return clampUnitInterval(confidence*0.65 + statusPrior*0.35)
}

func rspBeliefStatusContribution(status string) (float64, float64, float64) {
	switch normalizeKnowledgeClaimStatus(status) {
	case "CONFIRMED":
		return 1.00, 0, 1.00
	case "ACTIVE":
		return 0.25, 0, 0.35
	case "REVIEW":
		return 0.10, 0.15, 0.40
	case "STALE":
		return 0, 0.75, 0.75
	case "DISPUTED":
		return 0, 1.00, 1.00
	case "SUPERSEDED":
		return 0, 1.10, 1.00
	default:
		return 0, 0, 0.20
	}
}

func rspBeliefRelationContribution(domain, claimID string, relation KnowledgeClaimRelationRecord) (float64, float64, float64, bool, bool) {
	weight := clampUnitInterval(relation.Weight)
	if weight == 0 {
		weight = 1
	}
	fromSelf := strings.TrimSpace(relation.FromClaimID) == strings.TrimSpace(claimID)
	toSelf := strings.TrimSpace(relation.ToClaimID) == strings.TrimSpace(claimID)
	if !fromSelf && !toSelf {
		return 0, 0, 0, false, false
	}

	support := 0.0
	challenge := 0.0
	repair := 0.0
	verifierFresh := false
	supersededRisk := false

	switch domain {
	case "FACT":
		switch relation.RelationType {
		case "VALIDATED_BY":
			if fromSelf {
				support += 1.10 * weight
				verifierFresh = true
			}
		case "SUPPORTS":
			if fromSelf || toSelf {
				support += 0.75 * weight
			}
		case "CONTRADICTS":
			if toSelf {
				challenge += 1.00 * weight
			}
		case "SUPERSEDES":
			if toSelf {
				challenge += 1.15 * weight
				supersededRisk = true
			}
		}
	case "DECISION":
		switch relation.RelationType {
		case "VALIDATED_BY":
			if fromSelf {
				support += 0.90 * weight
				verifierFresh = true
			}
		case "SUPPORTS":
			if fromSelf || toSelf {
				support += 0.60 * weight
			}
		case "CONTRADICTS":
			if toSelf {
				challenge += 0.95 * weight
			}
		case "SUPERSEDES":
			if toSelf {
				challenge += 1.15 * weight
				supersededRisk = true
			}
		}
	case "BLOCKER":
		switch relation.RelationType {
		case "BLOCKS":
			if fromSelf {
				support += 0.95 * weight
			}
		case "VALIDATED_BY":
			if fromSelf {
				support += 0.60 * weight
				verifierFresh = true
			}
		case "SUPPORTS":
			if fromSelf || toSelf {
				support += 0.55 * weight
			}
		case "CONTRADICTS":
			if toSelf {
				challenge += 0.90 * weight
			}
		case "RESOLVES":
			if toSelf {
				repair += 1.10 * weight
			}
		case "SUPERSEDES":
			if toSelf {
				challenge += 0.75 * weight
				supersededRisk = true
			}
		}
	}
	return support, challenge, repair, verifierFresh, supersededRisk
}

func rspBeliefSuggestedState(domain, status string, posterior, uncertainty, drift, contradictionPressure float64, verifierFresh, supersededRisk, resolving bool) string {
	status = normalizeKnowledgeClaimStatus(status)
	switch domain {
	case "FACT":
		switch {
		case posterior >= 0.95 && uncertainty <= 0.45 && drift <= 0.15 && verifierFresh:
			return "VERIFIED"
		case posterior >= 0.75 && contradictionPressure < 0.45 && drift <= 0.35:
			return "SUPPORTED"
		case posterior < 0.45 || contradictionPressure > 0.45 || drift > 0.45 || status == "DISPUTED" || status == "SUPERSEDED":
			return "DISPUTED"
		default:
			return "ALLEGED"
		}
	case "DECISION":
		switch {
		case supersededRisk:
			if drift > 0.45 || status == "STALE" || posterior < 0.45 {
				return "STALE"
			}
			return "SUPERSEDED_RISK"
		case drift > 0.35 || status == "STALE":
			return "STALE"
		case posterior >= 0.75 && uncertainty <= 0.50 && contradictionPressure < 0.45:
			return "OPERATIVE"
		default:
			return "QUESTIONED"
		}
	case "BLOCKER":
		switch {
		case posterior < 0.15:
			return "INACTIVE"
		case drift > 0.45 || status == "STALE" || resolving:
			return "RESOLVING"
		case posterior >= 0.75 && contradictionPressure < 0.50:
			return "ACTIVE"
		default:
			return "WATCH"
		}
	default:
		return "UNSPECIFIED"
	}
}

func rspBeliefReportLess(left, right RSPBeliefClaimReport) bool {
	leftStable := rspBeliefSuggestedStateStable(left.SuggestedState)
	rightStable := rspBeliefSuggestedStateStable(right.SuggestedState)
	if leftStable != rightStable {
		return !leftStable
	}
	if left.DriftScore != right.DriftScore {
		return left.DriftScore > right.DriftScore
	}
	if left.ContradictionPressure != right.ContradictionPressure {
		return left.ContradictionPressure > right.ContradictionPressure
	}
	if left.ClaimUpdatedAt != right.ClaimUpdatedAt {
		return left.ClaimUpdatedAt > right.ClaimUpdatedAt
	}
	return left.ClaimID > right.ClaimID
}

func rspBeliefSuggestedStateStable(state string) bool {
	switch strings.TrimSpace(state) {
	case "VERIFIED", "SUPPORTED", "OPERATIVE", "ACTIVE":
		return true
	default:
		return false
	}
}

func rspBeliefClaimStatusPriority(status string) int {
	switch normalizeKnowledgeClaimStatus(status) {
	case "CONFIRMED":
		return 6
	case "ACTIVE":
		return 5
	case "REVIEW":
		return 4
	case "STALE":
		return 3
	case "DISPUTED":
		return 2
	case "SUPERSEDED":
		return 1
	default:
		return 0
	}
}

func (s *Store) loadRSPBeliefPeerClaims(ctx context.Context, workspaceID, claimID string, relations []KnowledgeClaimRelationRecord) (map[string]KnowledgeClaimRecord, error) {
	peers := make(map[string]KnowledgeClaimRecord)
	for _, relation := range relations {
		peerID := rspBeliefRelationPeerClaimID(relation, claimID)
		if peerID == "" {
			continue
		}
		if _, ok := peers[peerID]; ok {
			continue
		}
		peer, err := s.GetKnowledgeClaim(ctx, workspaceID, peerID)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "not found") {
				continue
			}
			return nil, err
		}
		peers[peerID] = peer
	}
	return peers, nil
}

func rspBeliefClaimGroupKey(claim KnowledgeClaimRecord) string {
	switch {
	case strings.TrimSpace(claim.SourceKind) != "" && strings.TrimSpace(claim.SourceID) != "":
		return rspBeliefClaimSourceGroupKey(claim)
	case strings.TrimSpace(claim.AgentID) != "":
		return rspBeliefClaimSourceGroupKey(claim)
	default:
		return "claim:" + strings.TrimSpace(claim.ClaimID)
	}
}

func (s *Store) rspBeliefClaimRootCauseGroups(ctx context.Context, claim KnowledgeClaimRecord) []string {
	groups, _ := s.rspResolveRootCauseGroupsForSource(ctx, claim.WorkspaceID, claim.SourceKind, claim.SourceID)
	return groups
}

func rspBeliefClaimSourceGroupKey(claim KnowledgeClaimRecord) string {
	switch {
	case strings.TrimSpace(claim.MemoryID) != "":
		return "memory:" + strings.TrimSpace(claim.MemoryID)
	case strings.TrimSpace(claim.SourceKind) != "" && strings.TrimSpace(claim.SourceID) != "":
		return "source:" + strings.TrimSpace(claim.SourceKind) + ":" + strings.TrimSpace(claim.SourceID)
	case strings.TrimSpace(claim.AgentID) != "" && strings.TrimSpace(claim.TaskID) != "":
		return "agent-task:" + strings.TrimSpace(claim.AgentID) + ":" + strings.TrimSpace(claim.TaskID)
	case strings.TrimSpace(claim.AgentID) != "":
		return "agent:" + strings.TrimSpace(claim.AgentID)
	default:
		return "claim:" + strings.TrimSpace(claim.ClaimID)
	}
}

func rspBeliefClaimIndependenceGroupKey(claim KnowledgeClaimRecord) string {
	targets := knowledgeClaimRelationTargets(claim)
	if len(targets) > 0 {
		sort.Strings(targets)
		return "claim-lineage:" + strings.Join(targets, "|")
	}
	return rspBeliefClaimSourceGroupKey(claim)
}

func rspBeliefEvidenceGroupKey(evidence string) string {
	prefix, remainder, found := strings.Cut(strings.TrimSpace(evidence), ":")
	if !found {
		return "evidence:" + strings.TrimSpace(evidence)
	}
	return "evidence:" + strings.TrimSpace(prefix) + ":" + strings.TrimSpace(remainder)
}

func rspBeliefRelationPeerClaimID(relation KnowledgeClaimRelationRecord, claimID string) string {
	claimID = strings.TrimSpace(claimID)
	fromClaimID := strings.TrimSpace(relation.FromClaimID)
	toClaimID := strings.TrimSpace(relation.ToClaimID)
	switch {
	case fromClaimID == claimID && toClaimID != "":
		return toClaimID
	case toClaimID == claimID && fromClaimID != "":
		return fromClaimID
	default:
		return ""
	}
}

func rspBeliefRelationGroupKey(relation KnowledgeClaimRelationRecord, claimID string, peerClaims map[string]KnowledgeClaimRecord) string {
	if peerID := rspBeliefRelationPeerClaimID(relation, claimID); peerID != "" {
		if peer, ok := peerClaims[peerID]; ok {
			return rspBeliefClaimIndependenceGroupKey(peer)
		}
		return "peer-claim:" + peerID
	}
	switch {
	case strings.TrimSpace(relation.SourceKind) != "" && strings.TrimSpace(relation.SourceID) != "":
		return "relation-source:" + strings.TrimSpace(relation.SourceKind) + ":" + strings.TrimSpace(relation.SourceID)
	case strings.TrimSpace(relation.FromClaimID) != "":
		return "relation-claim:" + strings.TrimSpace(relation.FromClaimID)
	default:
		return "relation:" + strings.TrimSpace(relation.RelationID)
	}
}

func rspBeliefRelationSourceGroupKey(relation KnowledgeClaimRelationRecord, claimID string, peerClaims map[string]KnowledgeClaimRecord) string {
	if peerID := rspBeliefRelationPeerClaimID(relation, claimID); peerID != "" {
		if peer, ok := peerClaims[peerID]; ok {
			return rspBeliefClaimSourceGroupKey(peer)
		}
		return "peer-claim:" + peerID
	}
	switch {
	case strings.TrimSpace(relation.SourceKind) != "" && strings.TrimSpace(relation.SourceID) != "":
		return "relation-source:" + strings.TrimSpace(relation.SourceKind) + ":" + strings.TrimSpace(relation.SourceID)
	case strings.TrimSpace(relation.FromClaimID) != "":
		return "relation-claim:" + strings.TrimSpace(relation.FromClaimID)
	default:
		return "relation:" + strings.TrimSpace(relation.RelationID)
	}
}

func (s *Store) rspBeliefEvidenceRootCauseGroups(ctx context.Context, workspaceID, evidence string) []string {
	groups, _ := s.rspResolveRootCauseGroupsForRef(ctx, workspaceID, evidence)
	return groups
}

func (s *Store) rspBeliefRelationRootCauseGroups(ctx context.Context, workspaceID string, relation KnowledgeClaimRelationRecord, claimID string, peerClaims map[string]KnowledgeClaimRecord) []string {
	if peerID := rspBeliefRelationPeerClaimID(relation, claimID); peerID != "" {
		if peer, ok := peerClaims[peerID]; ok {
			if groups := s.rspBeliefClaimRootCauseGroups(ctx, peer); len(groups) > 0 {
				return groups
			}
		}
	}
	groups, _ := s.rspResolveRootCauseGroupsForSource(ctx, workspaceID, relation.SourceKind, relation.SourceID)
	return groups
}

func mapKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		keys = append(keys, value)
	}
	return keys
}

func rspBeliefEvidenceRefs(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) > 16 {
		out = out[:16]
	}
	return out
}

func rspBeliefSnapshotPayload(report RSPBeliefReport) map[string]any {
	return map[string]any{
		"workspace_id":       report.WorkspaceID,
		"time_authority":     report.TimeAuthority,
		"claim_type":         report.ClaimType,
		"agent_id":           report.AgentID,
		"session_id":         report.SessionID,
		"task_id":            report.TaskID,
		"signal_type":        report.SignalType,
		"shadow_phase":       report.ShadowPhase,
		"calibration":        report.Calibration,
		"count":              report.Count,
		"stable_count":       report.StableCount,
		"needs_review_count": report.NeedsReviewCount,
		"high_drift_count":   report.HighDriftCount,
		"capability_flags":   report.CapabilityFlags,
		"items":              report.Items,
		"typed_event_type":   "RSP_BELIEF_SNAPSHOT",
		"summary":            report.Summary,
	}
}

func rspBeliefSnapshotEntityID(report RSPBeliefReport) string {
	parts := []string{"rspbelief", report.WorkspaceID}
	if report.ClaimType != "" {
		parts = append(parts, normalizeKnowledgeClaimType(report.ClaimType))
	}
	if report.AgentID != "" {
		parts = append(parts, report.AgentID)
	}
	if report.SessionID != "" {
		parts = append(parts, report.SessionID)
	}
	if report.TaskID != "" {
		parts = append(parts, report.TaskID)
	}
	return strings.Join(parts, ":")
}

func rspBeliefLogit(p float64) float64 {
	p = rspBeliefClamp(0.001, p, 0.999)
	return math.Log(p / (1 - p))
}

func rspBeliefSigmoid(v float64) float64 {
	return 1 / (1 + math.Exp(-v))
}

func rspBeliefClamp(minimum, value, maximum float64) float64 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}
