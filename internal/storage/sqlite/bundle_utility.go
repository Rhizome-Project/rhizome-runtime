package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// BundleUtilityParams holds the purely mathematical (non-LLM) inputs for U(B) evaluation.
type BundleUtilityParams struct {
	BaseValue                      float64 // Sum of FitScores or expected improvement
	UnlockScore                    float64 // Normalized unlock effect in [0, 1]
	CoverageScore                  float64 // Normalized coverage effect in [0, 1]
	RedundancyScore                float64 // Normalized overlap / redundancy penalty in [0, 1]
	SoftConflictScore              float64 // Normalized soft-conflict penalty in [0, 1]
	LeaseRiskScore                 float64 // Normalized lease / stale-base / linked-update risk in [0, 1]
	HardConflict                   bool    // Binary hard-constraint violation
	ContradictEdgesCount           int     // |E_contradicts| in RMP layer
	VerifierFailsCount             int     // AST or Unit Test failures
	AlphaUnlock                    float64 // Weight for unlock effect
	AlphaCoverage                  float64 // Weight for coverage effect
	AlphaRedundancy                float64 // Weight for redundancy / overlap penalty
	AlphaSoftConflict              float64 // Weight for soft-conflict penalty
	AlphaLeaseRisk                 float64 // Weight for lease-risk penalty
	Lambda1                        float64 // Weight for contradicts
	Lambda2                        float64 // Weight for verifier fails
	AdmissionRedundancyThreshold   float64 // Maximum redundancy allowed for merge admission
	AdmissionSoftConflictThreshold float64 // Maximum soft-conflict level allowed for merge admission
	AdmissionLeaseRiskThreshold    float64 // Maximum lease risk allowed for merge admission
	AdmissionCombinedRiskThreshold float64 // Maximum blended overlap risk allowed for merge admission
	MergeBaseThreshold             float64 // Minimum direct base value to satisfy merge gate
	MergeUnlockThreshold           float64 // Minimum unlock score to satisfy merge gate
	MergeCoverageThreshold         float64 // Minimum coverage score to satisfy merge gate
	MergeThreshold                 float64 // If U(B) > MergeThreshold, we MERGE
}

// DefaultBundleParams provide the baseline tuning for U(B).
var DefaultBundleParams = BundleUtilityParams{
	AlphaUnlock:                    0.35,
	AlphaCoverage:                  0.35,
	AlphaRedundancy:                0.25,
	AlphaSoftConflict:              0.30,
	AlphaLeaseRisk:                 0.35,
	Lambda1:                        0.5,
	Lambda2:                        2.0, // Verifier fails are heavily penalized
	AdmissionRedundancyThreshold:   0.85,
	AdmissionSoftConflictThreshold: 0.75,
	AdmissionLeaseRiskThreshold:    0.70,
	AdmissionCombinedRiskThreshold: 0.75,
	MergeBaseThreshold:             0.15,
	MergeUnlockThreshold:           0.25,
	MergeCoverageThreshold:         0.25,
	MergeThreshold:                 0.1, // Minimum utility for MERGE
}

// BundleDecision encapsulates the result of the U(B) evaluation.
type BundleDecision struct {
	UtilityScore       float64
	Decision           string // "MERGE" | "FORK"
	Reasoning          string
	BenefitScore       float64
	PenaltyScore       float64
	SupportClass       string
	GateSatisfied      bool
	GateReason         string
	AdmissionSatisfied bool
	AdmissionReason    string
	AdmissionClass     string
	RebaseAdmissible   bool
	RebaseReason       string
	RebasePlanClass    string
	ConflictSafeClass  string
	NextAction         string
	HardConflict       bool
	DecisionReason     string
	DecisionDetail     string
	MergeClass         string
}

func bundleUtilityNormalizedSignal(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func bundleUtilityMergeGate(params BundleUtilityParams) (bool, string) {
	if params.MergeBaseThreshold <= 0 && params.MergeUnlockThreshold <= 0 && params.MergeCoverageThreshold <= 0 {
		return true, "disabled"
	}
	if params.BaseValue >= params.MergeBaseThreshold {
		return true, "base"
	}
	if bundleUtilityNormalizedSignal(params.UnlockScore) >= params.MergeUnlockThreshold {
		return true, "unlock"
	}
	if bundleUtilityNormalizedSignal(params.CoverageScore) >= params.MergeCoverageThreshold {
		return true, "coverage"
	}
	return false, "unmet"
}

func bundleUtilityAdmissionRisk(params BundleUtilityParams) (bool, string) {
	redundancy := bundleUtilityNormalizedSignal(params.RedundancyScore)
	softConflict := bundleUtilityNormalizedSignal(params.SoftConflictScore)
	leaseRisk := bundleUtilityNormalizedSignal(params.LeaseRiskScore)
	combinedRisk := 0.30*redundancy + 0.30*softConflict + 0.40*leaseRisk

	if params.AdmissionRedundancyThreshold <= 0 &&
		params.AdmissionSoftConflictThreshold <= 0 &&
		params.AdmissionLeaseRiskThreshold <= 0 &&
		params.AdmissionCombinedRiskThreshold <= 0 {
		return true, "disabled"
	}
	if params.AdmissionLeaseRiskThreshold > 0 && leaseRisk >= params.AdmissionLeaseRiskThreshold {
		return false, "lease_risk"
	}
	if params.AdmissionSoftConflictThreshold > 0 && softConflict >= params.AdmissionSoftConflictThreshold {
		return false, "soft_conflict"
	}
	if params.AdmissionRedundancyThreshold > 0 && redundancy >= params.AdmissionRedundancyThreshold {
		return false, "redundancy"
	}
	if params.AdmissionCombinedRiskThreshold > 0 && combinedRisk >= params.AdmissionCombinedRiskThreshold {
		return false, "combined_risk"
	}
	return true, "clear"
}

func bundleUtilityAdmissionClass(params BundleUtilityParams, admissionSatisfied bool, admissionReason string) string {
	if !admissionSatisfied {
		return "rejected_" + admissionReason
	}
	if params.AdmissionRedundancyThreshold <= 0 &&
		params.AdmissionSoftConflictThreshold <= 0 &&
		params.AdmissionLeaseRiskThreshold <= 0 &&
		params.AdmissionCombinedRiskThreshold <= 0 {
		return "disabled"
	}

	redundancy := bundleUtilityNormalizedSignal(params.RedundancyScore)
	softConflict := bundleUtilityNormalizedSignal(params.SoftConflictScore)
	leaseRisk := bundleUtilityNormalizedSignal(params.LeaseRiskScore)
	combinedRisk := 0.30*redundancy + 0.30*softConflict + 0.40*leaseRisk

	ratio := func(value, threshold float64) float64 {
		if threshold <= 0 {
			return 0
		}
		return value / threshold
	}

	redundancyRatio := ratio(redundancy, params.AdmissionRedundancyThreshold)
	softConflictRatio := ratio(softConflict, params.AdmissionSoftConflictThreshold)
	leaseRiskRatio := ratio(leaseRisk, params.AdmissionLeaseRiskThreshold)
	combinedRiskRatio := ratio(combinedRisk, params.AdmissionCombinedRiskThreshold)

	const guardedCutoff = 0.85
	const bufferedCutoff = 0.50

	if leaseRiskRatio >= guardedCutoff {
		return "guarded_lease_risk"
	}
	if softConflictRatio >= guardedCutoff {
		return "guarded_soft_conflict"
	}
	if redundancyRatio >= guardedCutoff {
		return "guarded_redundancy"
	}
	if combinedRiskRatio >= guardedCutoff {
		return "guarded_combined_risk"
	}
	if leaseRiskRatio >= bufferedCutoff ||
		softConflictRatio >= bufferedCutoff ||
		redundancyRatio >= bufferedCutoff ||
		combinedRiskRatio >= bufferedCutoff {
		return "buffered"
	}
	return "clear"
}

func bundleUtilityThresholdFailureDetail(
	params BundleUtilityParams,
	totalBenefit float64,
	penaltyRedundancy float64,
	penaltySoftConflict float64,
	penaltyLeaseRisk float64,
	penaltyContradicts float64,
	penaltyVerifier float64,
) string {
	if totalBenefit <= params.MergeThreshold {
		return "benefit_gap"
	}

	type penaltyDriver struct {
		label string
		value float64
	}

	drivers := []penaltyDriver{
		{label: "verifier_fails", value: penaltyVerifier},
		{label: "contradict_edges", value: penaltyContradicts},
		{label: "lease_risk", value: penaltyLeaseRisk},
		{label: "soft_conflict", value: penaltySoftConflict},
		{label: "redundancy", value: penaltyRedundancy},
	}

	strongest := penaltyDriver{label: "penalty_mix", value: 0}
	for _, driver := range drivers {
		if driver.value > strongest.value {
			strongest = driver
		}
	}
	if strongest.value <= 0 {
		return "benefit_gap"
	}
	return strongest.label
}

func bundleUtilityMergeClass(params BundleUtilityParams, gateReason string, unlockBonus, coverageBonus float64) string {
	switch gateReason {
	case "coverage":
		return "coverage_gate"
	case "unlock":
		return "unlock_gate"
	case "base":
		return "base_gate"
	}

	if unlockBonus > coverageBonus && unlockBonus > 0 {
		return "ungated_unlock_support"
	}
	if coverageBonus > unlockBonus && coverageBonus > 0 {
		return "ungated_coverage_support"
	}
	if unlockBonus > 0 || coverageBonus > 0 {
		return "ungated_balanced_support"
	}
	if params.BaseValue > 0 {
		return "ungated_base_support"
	}
	return "none"
}

func bundleUtilitySupportClass(unlockBonus, coverageBonus float64) string {
	totalSupport := unlockBonus + coverageBonus
	if totalSupport <= 0 {
		return "base_only"
	}
	if unlockBonus <= 0 {
		return "coverage_only"
	}
	if coverageBonus <= 0 {
		return "unlock_only"
	}

	unlockShare := unlockBonus / totalSupport
	coverageShare := coverageBonus / totalSupport

	const dominantCutoff = 0.60
	if unlockShare >= dominantCutoff {
		return "unlock_dominant"
	}
	if coverageShare >= dominantCutoff {
		return "coverage_dominant"
	}
	return "balanced"
}

func bundleUtilityRebaseAdmission(params BundleUtilityParams, decision, decisionReason string) (bool, string) {
	if decision != "FORK" {
		return false, "not_needed"
	}

	switch decisionReason {
	case "admission_redundancy", "admission_soft_conflict", "admission_combined_risk":
	default:
		return false, "not_overlap_admission"
	}

	if params.ContradictEdgesCount > 0 {
		return false, "contradict_edges"
	}
	if params.VerifierFailsCount > 0 {
		return false, "verifier_fails"
	}

	leaseRisk := bundleUtilityNormalizedSignal(params.LeaseRiskScore)
	rebaseLeaseCap := 0.45
	if params.AdmissionLeaseRiskThreshold > 0 {
		rebaseLeaseCap = math.Min(rebaseLeaseCap, params.AdmissionLeaseRiskThreshold*0.75)
	}
	if leaseRisk > rebaseLeaseCap {
		return false, "lease_risk"
	}

	return true, "clear"
}

func bundleUtilityConflictSafeClass(decision, decisionReason, admissionClass string, rebaseAdmissible bool) string {
	if decision == "MERGE" {
		if strings.HasPrefix(admissionClass, "guarded_") || admissionClass == "buffered" {
			return "merge_guarded"
		}
		return "merge_direct"
	}

	switch decisionReason {
	case "admission_redundancy", "admission_soft_conflict", "admission_combined_risk":
		if rebaseAdmissible {
			return "rebase_candidate"
		}
		return "fork_required"
	default:
		return "fork_required"
	}
}

func bundleUtilityRebasePlanClass(decisionReason string, rebaseAdmissible bool) string {
	if !rebaseAdmissible {
		return "none"
	}

	switch decisionReason {
	case "admission_redundancy":
		return "trim_redundancy"
	case "admission_soft_conflict":
		return "reorder_soft_conflict"
	case "admission_combined_risk":
		return "split_overlap_cluster"
	default:
		return "none"
	}
}

func bundleUtilityNextAction(decision string, rebaseAdmissible bool) string {
	if decision == "MERGE" {
		return model.RebaseNextActionMerge
	}
	if rebaseAdmissible {
		return model.RebaseNextActionAttempt
	}
	return model.RebaseNextActionHard
}

func bundleUtilityDecisionScore(utility float64) int {
	if math.IsNaN(utility) || math.IsInf(utility, 0) {
		return 0
	}
	return int(math.Round(bundleUtilityNormalizedSignal(utility) * 100))
}

func bundleUtilityForkArtifactRefs(alternativePatchRef string) []string {
	refs := []string{}
	if trimmed := strings.TrimSpace(alternativePatchRef); trimmed != "" {
		refs = append(refs, trimmed)
	}
	return uniqueSortedStrings(refs)
}

func bundleUtilityForkConstraintRefs(decision BundleDecision) []string {
	refs := []string{
		"bundle_decision:" + strings.TrimSpace(decision.DecisionReason),
		"bundle_detail:" + strings.TrimSpace(decision.DecisionDetail),
		"conflict_safe:" + strings.TrimSpace(decision.ConflictSafeClass),
		"next_action:" + strings.TrimSpace(decision.NextAction),
	}
	if trimmed := strings.TrimSpace(decision.RebaseReason); trimmed != "" && trimmed != "not_needed" {
		refs = append(refs, "rebase_reason:"+trimmed)
	}
	if trimmed := strings.TrimSpace(decision.RebasePlanClass); trimmed != "" && trimmed != "none" {
		refs = append(refs, "rebase_plan:"+trimmed)
	}
	return uniqueSortedStrings(refs)
}

func bundleUtilityForkEvidenceRecords(tensionID, workspaceID string, artifactRefs, constraintRefs []string, createdAt string) []TensionEvidenceRecord {
	out := make([]TensionEvidenceRecord, 0, len(artifactRefs)+len(constraintRefs))
	for _, ref := range uniqueSortedStrings(artifactRefs) {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "bundle_artifact",
			EvidenceRef:  ref,
			EventID:      ref,
			Weight:       4,
			Summary:      "Fork candidate alternative patch reference",
			CreatedAt:    createdAt,
		})
	}
	for _, ref := range uniqueSortedStrings(constraintRefs) {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "bundle_decision",
			EvidenceRef:  ref,
			EventID:      ref,
			Weight:       3,
			Summary:      "Fork candidate bundle decision envelope",
			CreatedAt:    createdAt,
		})
	}
	return out
}

func bundleUtilityShouldMaterializeRebaseFollowup(decision BundleDecision) bool {
	return strings.TrimSpace(decision.NextAction) == model.RebaseNextActionAttempt &&
		strings.TrimSpace(decision.ConflictSafeClass) == "rebase_candidate" &&
		strings.TrimSpace(decision.RebasePlanClass) != "" &&
		strings.TrimSpace(decision.RebasePlanClass) != "none"
}

func bundleUtilityRebaseFollowupQueueKey(tensionID string) string {
	if trimmed := strings.TrimSpace(tensionID); trimmed != "" {
		return model.RebaseFollowupQueueKeyPrefix + trimmed
	}
	return "tension_rebase_followup"
}

func bundleUtilityRebaseFollowupConstraintRefs(parentTensionID string, decision BundleDecision) []string {
	refs := []string{
		"parent_tension:" + strings.TrimSpace(parentTensionID),
		"bundle_decision:" + strings.TrimSpace(decision.DecisionReason),
		"bundle_detail:" + strings.TrimSpace(decision.DecisionDetail),
		"conflict_safe:" + strings.TrimSpace(decision.ConflictSafeClass),
		"next_action:" + strings.TrimSpace(decision.NextAction),
		"rebase_plan:" + strings.TrimSpace(decision.RebasePlanClass),
	}
	if trimmed := strings.TrimSpace(decision.RebaseReason); trimmed != "" && trimmed != "not_needed" {
		refs = append(refs, "rebase_reason:"+trimmed)
	}
	return uniqueSortedStrings(refs)
}

func bundleUtilityRebaseFollowupQueueInput(
	workspaceID,
	coalitionID,
	parentTensionID,
	followupTensionID,
	taskID,
	agentID,
	queueKey,
	alternativePatchRef string,
	taskIDs []string,
	decision BundleDecision,
) OperatorQueueUpsertInput {
	lines := []string{
		fmt.Sprintf("Coalition ID: %s", strings.TrimSpace(coalitionID)),
		fmt.Sprintf("Fork candidate tension: %s", strings.TrimSpace(parentTensionID)),
		fmt.Sprintf("Repair tension: %s", strings.TrimSpace(followupTensionID)),
		fmt.Sprintf("Next action: %s", strings.TrimSpace(decision.NextAction)),
		fmt.Sprintf("Rebase plan: %s", strings.TrimSpace(decision.RebasePlanClass)),
		fmt.Sprintf("Rebase admission: %s", strings.TrimSpace(decision.RebaseReason)),
		fmt.Sprintf("Conflict-safe branch: %s", strings.TrimSpace(decision.ConflictSafeClass)),
		fmt.Sprintf("Decision reason: %s", strings.TrimSpace(decision.DecisionReason)),
	}
	if trimmed := strings.TrimSpace(alternativePatchRef); trimmed != "" {
		lines = append(lines, fmt.Sprintf("Alternative patch ref: %s", trimmed))
	}
	if trimmed := strings.TrimSpace(taskID); trimmed != "" {
		lines = append(lines, fmt.Sprintf("Task ID: %s", trimmed))
	}
	payload := model.RebaseFollowupPayload{
		CoalitionID:          strings.TrimSpace(coalitionID),
		ForkTensionID:        strings.TrimSpace(parentTensionID),
		RepairTensionID:      strings.TrimSpace(followupTensionID),
		StewardLeaseRequired: bundleUtilityRebaseFollowupNeedsStewardLease(workspaceID, taskID, agentID, taskIDs),
		NextAction:           strings.TrimSpace(decision.NextAction),
		RebasePlanClass:      strings.TrimSpace(decision.RebasePlanClass),
		RebaseReason:         strings.TrimSpace(decision.RebaseReason),
		ConflictSafeClass:    strings.TrimSpace(decision.ConflictSafeClass),
		DecisionReason:       strings.TrimSpace(decision.DecisionReason),
		DecisionDetail:       strings.TrimSpace(decision.DecisionDetail),
		AlternativePatch:     strings.TrimSpace(alternativePatchRef),
		TaskID:               strings.TrimSpace(taskID),
		TaskIDs:              uniqueSortedStrings(taskIDs),
	}
	payload.Normalize()
	return OperatorQueueUpsertInput{
		WorkspaceID:       strings.TrimSpace(workspaceID),
		QueueKey:          strings.TrimSpace(queueKey),
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           fmt.Sprintf("Rebase %s for coalition %s", strings.TrimSpace(decision.RebasePlanClass), strings.TrimSpace(coalitionID)),
		Details:           strings.Join(lines, "\n"),
		PayloadJSON:       mustJSON(payload),
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          strings.TrimSpace(followupTensionID),
		TaskID:            strings.TrimSpace(taskID),
		AgentID:           strings.TrimSpace(agentID),
		KeepSessionActive: true,
	}
}

func bundleUtilityProtoClusterID(workspaceID, parentProtoClusterID string, taskIDs []string) string {
	taskID := strings.TrimSpace(taskIDFromTaskIDs(taskIDs))
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID != "" && taskID != "" {
		return "task:" + workspaceID + "/" + taskID
	}
	return strings.TrimSpace(parentProtoClusterID)
}

func bundleUtilityRebaseFollowupNeedsStewardLease(workspaceID, taskID, agentID string, taskIDs []string) bool {
	clusterID := bundleUtilityProtoClusterID(workspaceID, "", append([]string{strings.TrimSpace(taskID)}, taskIDs...))
	return strings.HasPrefix(clusterID, "task:") && strings.TrimSpace(agentID) != ""
}

func (s *Store) ensureBundleRebaseFollowupStewardTx(ctx context.Context, tx *sql.Tx, clusterID, candidateID, epochID string) error {
	clusterID = strings.TrimSpace(clusterID)
	candidateID = strings.TrimSpace(candidateID)
	epochID = strings.TrimSpace(epochID)
	if clusterID == "" || candidateID == "" || epochID == "" {
		return errors.New("cluster_id, candidate_id, and epoch_id are required")
	}

	active, err := s.getActiveStewardTx(ctx, tx, clusterID)
	if err == nil {
		if strings.EqualFold(strings.TrimSpace(active.StewardAgentID), candidateID) {
			return nil
		}
		return fmt.Errorf(
			"%w: cluster %s active steward=%s requested=%s",
			ErrStewardshipActive,
			clusterID,
			strings.TrimSpace(active.StewardAgentID),
			candidateID,
		)
	}
	if !errors.Is(err, ErrStewardNotFound) {
		return err
	}
	_, err = s.electClusterStewardTx(ctx, tx, ElectStewardInput{
		ClusterID:   clusterID,
		EpochID:     epochID,
		CandidateID: candidateID,
		TTLSeconds:  300,
	})
	return err
}

func (s *Store) bundleUtilityCoalitionTaskContextTx(ctx context.Context, tx *sql.Tx, workspaceID, coalitionID string) ([]string, string, string) {
	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil || coalition == nil {
		return nil, "", ""
	}
	sourceTensionID := strings.TrimSpace(coalition.TensionID)
	if sourceTensionID == "" {
		return nil, "", ""
	}
	record, err := s.loadTensionRecord(ctx, tx, workspaceID, sourceTensionID)
	if err != nil {
		return nil, "", ""
	}
	taskIDs := uniqueSortedStrings(record.TaskIDs)
	if len(taskIDs) == 0 {
		return nil, "", ""
	}
	return taskIDs, taskIDs[0], agentIDFromAgentIDs(record.AgentIDs)
}

func taskIDFromTaskIDs(taskIDs []string) string {
	for _, taskID := range taskIDs {
		if trimmed := strings.TrimSpace(taskID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func agentIDFromAgentIDs(agentIDs []string) string {
	for _, agentID := range agentIDs {
		if trimmed := strings.TrimSpace(agentID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func bundleUtilityRebaseFollowupEvidenceRecords(
	tensionID,
	workspaceID,
	parentTensionID,
	parentEventID string,
	artifactRefs,
	constraintRefs []string,
	createdAt string,
) []TensionEvidenceRecord {
	out := make([]TensionEvidenceRecord, 0, 2+len(artifactRefs)+len(constraintRefs))
	if trimmed := strings.TrimSpace(parentTensionID); trimmed != "" {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "parent_tension",
			EvidenceRef:  trimmed,
			EventID:      trimmed,
			Weight:       6,
			Summary:      "Parent fork candidate for bounded overlap rebase follow-up",
			CreatedAt:    createdAt,
		})
	}
	if trimmed := strings.TrimSpace(parentEventID); trimmed != "" {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "runtime_event",
			EvidenceRef:  trimmed,
			EventID:      trimmed,
			Weight:       5,
			Summary:      "Fork runtime event that triggered bounded overlap rebase follow-up",
			CreatedAt:    createdAt,
		})
	}
	for _, ref := range uniqueSortedStrings(artifactRefs) {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "bundle_artifact",
			EvidenceRef:  ref,
			EventID:      ref,
			Weight:       4,
			Summary:      "Rebase follow-up alternative patch reference",
			CreatedAt:    createdAt,
		})
	}
	for _, ref := range uniqueSortedStrings(constraintRefs) {
		out = append(out, TensionEvidenceRecord{
			TensionID:    strings.TrimSpace(tensionID),
			WorkspaceID:  strings.TrimSpace(workspaceID),
			EvidenceKind: "bundle_decision",
			EvidenceRef:  ref,
			EventID:      ref,
			Weight:       3,
			Summary:      "Rebase follow-up decision envelope",
			CreatedAt:    createdAt,
		})
	}
	return out
}

func (s *Store) generateRebaseFollowupTensionTx(
	ctx context.Context,
	tx *sql.Tx,
	authority WorkspaceAuthorityRecord,
	workspaceID,
	coalitionID,
	alternativePatchRef string,
	taskIDs []string,
	parent TensionRecord,
	parentEvent RuntimeEventRecord,
	decision BundleDecision,
	now string,
) error {
	if !bundleUtilityShouldMaterializeRebaseFollowup(decision) {
		return nil
	}

	followupID := nextID("tens")
	queueKey := bundleUtilityRebaseFollowupQueueKey(followupID)
	artifactRefs := bundleUtilityForkArtifactRefs(alternativePatchRef)
	constraintRefs := bundleUtilityRebaseFollowupConstraintRefs(parent.TensionID, decision)
	constraintRefs = append(constraintRefs, "queue:"+queueKey)
	constraintRefs = uniqueSortedStrings(constraintRefs)
	evidence := bundleUtilityRebaseFollowupEvidenceRecords(
		followupID,
		workspaceID,
		parent.TensionID,
		parentEvent.EventID,
		artifactRefs,
		constraintRefs,
		now,
	)
	evidenceRefs := evidenceRefsFromEvidence(evidence)
	baseScore := tensionBaseScore("repair", len(evidence), false, false)

	followup := TensionRecord{
		TensionID:       followupID,
		WorkspaceID:     workspaceID,
		ProtoClusterID:  bundleUtilityProtoClusterID(workspaceID, parent.ProtoClusterID, taskIDs),
		TensionType:     normalizeTensionType("repair"),
		LifecycleState:  tensionLifecycleEmergent,
		ReviewStatus:    tensionReviewPending,
		Title:           fmt.Sprintf("Coalition Rebase (%s): %s", decision.RebasePlanClass, strings.TrimSpace(coalitionID)),
		Summary:         fmt.Sprintf("Bounded overlap rebase follow-up for fork candidate %s. Next action: %s. Rebase admission: %s. Alternative Patch Ref: %s.", parent.TensionID, decision.NextAction, decision.RebaseReason, strings.TrimSpace(alternativePatchRef)),
		AnchorKind:      "coalition_id",
		AnchorRef:       strings.TrimSpace(coalitionID),
		TaskIDs:         uniqueSortedStrings(taskIDs),
		AgentIDs:        uniqueSortedStrings(parent.AgentIDs),
		ArtifactRefs:    artifactRefs,
		ConstraintRefs:  constraintRefs,
		BaseScore:       baseScore,
		SurfaceScore:    baseScore,
		EvidenceCount:   len(evidence),
		LastSeenEventID: lastGovernedTensionEvidenceRef(evidenceRefs),
		LastSeenAt:      now,
		LastDetectedAt:  now,
		LastRefreshedAt: now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := s.upsertTensionTx(ctx, tx, followup); err != nil {
		return err
	}
	if err := s.upsertTensionEvidenceTx(ctx, tx, workspaceID, followup.TensionID, evidence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_tension_dependencies (workspace_id, tension_id, depends_on_tension_id, dependency_type, created_at)
		  VALUES (?, ?, ?, ?, ?)
		  ON CONFLICT DO NOTHING`,
		workspaceID,
		parent.TensionID,
		followup.TensionID,
		"BLOCKS",
		now,
	); err != nil {
		return fmt.Errorf("insert rebase follow-up dependency: %w", err)
	}

	payload := tensionRuntimeEventPayload(followup, evidenceRefs, "tension.emerged", "Created as bounded overlap rebase follow-up")
	payload["coalition_id"] = strings.TrimSpace(coalitionID)
	payload["parent_tension_id"] = parent.TensionID
	payload["source_event_id"] = parentEvent.EventID
	payload["next_action"] = decision.NextAction
	payload["rebase_reason"] = decision.RebaseReason
	payload["rebase_plan_class"] = decision.RebasePlanClass
	payload["conflict_safe_class"] = decision.ConflictSafeClass
	payload["queue_key"] = queueKey
	if len(taskIDs) > 0 {
		payload["task_ids"] = uniqueSortedStrings(taskIDs)
		payload["task_id"] = strings.TrimSpace(taskIDs[0])
	}
	_, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID:    workspaceID,
		EventType:      "tension.emerged",
		EntityType:     "tension",
		EntityID:       followup.TensionID,
		ActorType:      "system",
		ActorID:        "control_plane",
		ParentRefsJSON: runtimeEventParentRefsJSONForRecord(parentEvent.EventID),
		PayloadJSON:    mustJSON(payload),
		CreatedAt:      now,
	})
	if err != nil {
		return err
	}

	queueInput := bundleUtilityRebaseFollowupQueueInput(
		workspaceID,
		coalitionID,
		parent.TensionID,
		followup.TensionID,
		firstNonEmpty(taskIDFromTaskIDs(taskIDs), taskIDFromTaskIDs(parent.TaskIDs)),
		agentIDFromAgentIDs(parent.AgentIDs),
		queueKey,
		alternativePatchRef,
		taskIDs,
		decision,
	)
	stewardCandidateID := firstNonEmpty(strings.TrimSpace(queueInput.AgentID), agentIDFromAgentIDs(parent.AgentIDs))
	bundleClusterID := bundleUtilityProtoClusterID(workspaceID, parent.ProtoClusterID, taskIDs)
	if strings.HasPrefix(bundleClusterID, "task:") && strings.TrimSpace(stewardCandidateID) == "" {
		return errors.New("task-linked bundle rebase follow-up requires steward candidate")
	}
	stewardRequired := bundleUtilityRebaseFollowupNeedsStewardLease(workspaceID, queueInput.TaskID, stewardCandidateID, taskIDs)
	if stewardRequired {
		if err := s.ensureBundleRebaseFollowupStewardTx(
			ctx,
			tx,
			bundleClusterID,
			stewardCandidateID,
			followup.TensionID,
		); err != nil {
			return fmt.Errorf("ensure bundle rebase steward lease: %w", err)
		}
	}
	queueRecord, _, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(queueInput), now)
	if err != nil {
		return err
	}
	queuePayload := model.RebaseFollowupPayload{
		CoalitionID:          strings.TrimSpace(coalitionID),
		ForkTensionID:        parent.TensionID,
		RepairTensionID:      followup.TensionID,
		StewardLeaseRequired: stewardRequired,
		NextAction:           strings.TrimSpace(decision.NextAction),
		RebasePlanClass:      strings.TrimSpace(decision.RebasePlanClass),
		RebaseReason:         strings.TrimSpace(decision.RebaseReason),
		ConflictSafeClass:    strings.TrimSpace(decision.ConflictSafeClass),
		DecisionReason:       strings.TrimSpace(decision.DecisionReason),
		DecisionDetail:       strings.TrimSpace(decision.DecisionDetail),
		AlternativePatch:     strings.TrimSpace(alternativePatchRef),
		TaskID:               strings.TrimSpace(queueRecord.TaskID),
		TaskIDs:              uniqueSortedStrings(taskIDs),
	}
	queuePayload.Normalize()
	_, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID:    workspaceID,
		EventType:      "operator_queue.rebase_followup_created",
		EntityType:     "operator_queue",
		EntityID:       queueRecord.QueueID,
		ActorType:      "system",
		ActorID:        "control_plane",
		ParentRefsJSON: runtimeEventParentRefsJSONForRecord(parentEvent.EventID),
		PayloadJSON:    mustJSON(queuePayload),
		CreatedAt:      now,
	})
	return err
}

// CalculateBundleUtility mathematically evaluates whether a coalition's proposed bundle of claims/patches
// should be merged or rejected (forked), strictly avoiding LLM calls as per User Constraints.
func CalculateBundleUtility(params BundleUtilityParams) BundleDecision {
	// Bounded positive terms keep unlock/coverage visible without letting them exceed a full unit contribution.
	unlockBonus := params.AlphaUnlock * bundleUtilityNormalizedSignal(params.UnlockScore)
	coverageBonus := params.AlphaCoverage * bundleUtilityNormalizedSignal(params.CoverageScore)
	totalBenefit := params.BaseValue + unlockBonus + coverageBonus

	// Bounded overlap-side penalties keep redundancy and soft conflict visible before hard verifier failure dominates.
	penaltyRedundancy := params.AlphaRedundancy * bundleUtilityNormalizedSignal(params.RedundancyScore)
	penaltySoftConflict := params.AlphaSoftConflict * bundleUtilityNormalizedSignal(params.SoftConflictScore)
	penaltyLeaseRisk := params.AlphaLeaseRisk * bundleUtilityNormalizedSignal(params.LeaseRiskScore)

	// U(B) = Base Value + Unlock + Coverage - Redundancy - SoftConflict - LeaseRisk - lambda_1 * |E_contradicts| - lambda_2 * VerifierFails
	penaltyContradicts := params.Lambda1 * float64(params.ContradictEdgesCount)
	penaltyVerifier := params.Lambda2 * float64(params.VerifierFailsCount)
	totalPenalty := penaltyRedundancy + penaltySoftConflict + penaltyLeaseRisk + penaltyContradicts + penaltyVerifier

	utility := totalBenefit - totalPenalty
	gateSatisfied, gateReason := bundleUtilityMergeGate(params)
	admissionSatisfied, admissionReason := bundleUtilityAdmissionRisk(params)
	admissionClass := bundleUtilityAdmissionClass(params, admissionSatisfied, admissionReason)

	decision := "MERGE"
	decisionReason := "merge_allowed"
	decisionDetail := "merge_allowed"
	mergeClass := bundleUtilityMergeClass(params, gateReason, unlockBonus, coverageBonus)
	supportClass := bundleUtilitySupportClass(unlockBonus, coverageBonus)
	if params.HardConflict {
		decision = "FORK"
		decisionReason = "hard_conflict"
		decisionDetail = "hard_conflict"
		mergeClass = "none"
		supportClass = "none"
	} else if utility <= params.MergeThreshold {
		decision = "FORK"
		decisionReason = "utility_below_threshold"
		decisionDetail = bundleUtilityThresholdFailureDetail(
			params,
			totalBenefit,
			penaltyRedundancy,
			penaltySoftConflict,
			penaltyLeaseRisk,
			penaltyContradicts,
			penaltyVerifier,
		)
		mergeClass = "none"
		supportClass = "none"
	} else if !gateSatisfied {
		decision = "FORK"
		decisionReason = "gate_unmet"
		decisionDetail = gateReason
		mergeClass = "none"
		supportClass = "none"
	} else if !admissionSatisfied {
		decision = "FORK"
		decisionReason = "admission_" + admissionReason
		decisionDetail = admissionReason
		mergeClass = "none"
		supportClass = "none"
	}
	rebaseAdmissible, rebaseReason := bundleUtilityRebaseAdmission(params, decision, decisionReason)
	rebasePlanClass := bundleUtilityRebasePlanClass(decisionReason, rebaseAdmissible)
	conflictSafeClass := bundleUtilityConflictSafeClass(decision, decisionReason, admissionClass, rebaseAdmissible)
	nextAction := bundleUtilityNextAction(decision, rebaseAdmissible)

	// Avoid negative infinity or arbitrary formatting
	utility = math.Round(utility*1000) / 1000

	reasoning := fmt.Sprintf(
		"U(B)=%.3f (Base: %.2f, Benefit: %.2f [Unlock: %.2f, Coverage: %.2f], Penalty: %.2f [Redundancy: %.2f, SoftConflict: %.2f, LeaseRisk: %.2f, Contradicts: %d, Fails: %d], SupportClass: %s, Gate: %s, Admission: %s, AdmissionClass: %s, RebaseAdmissible: %t, RebaseReason: %s, RebasePlanClass: %s, ConflictSafeClass: %s, NextAction: %s, HardConflict: %t, Decision: %s, Detail: %s, MergeClass: %s)",
		utility,
		params.BaseValue,
		unlockBonus+coverageBonus,
		unlockBonus,
		coverageBonus,
		totalPenalty,
		penaltyRedundancy,
		penaltySoftConflict,
		penaltyLeaseRisk,
		params.ContradictEdgesCount,
		params.VerifierFailsCount,
		supportClass,
		gateReason,
		admissionReason,
		admissionClass,
		rebaseAdmissible,
		rebaseReason,
		rebasePlanClass,
		conflictSafeClass,
		nextAction,
		params.HardConflict,
		decisionReason,
		decisionDetail,
		mergeClass,
	)

	return BundleDecision{
		UtilityScore:       utility,
		Decision:           decision,
		Reasoning:          reasoning,
		BenefitScore:       unlockBonus + coverageBonus,
		PenaltyScore:       totalPenalty,
		SupportClass:       supportClass,
		GateSatisfied:      gateSatisfied,
		GateReason:         gateReason,
		AdmissionSatisfied: admissionSatisfied,
		AdmissionReason:    admissionReason,
		AdmissionClass:     admissionClass,
		RebaseAdmissible:   rebaseAdmissible,
		RebaseReason:       rebaseReason,
		RebasePlanClass:    rebasePlanClass,
		ConflictSafeClass:  conflictSafeClass,
		NextAction:         nextAction,
		HardConflict:       params.HardConflict,
		DecisionReason:     decisionReason,
		DecisionDetail:     decisionDetail,
		MergeClass:         mergeClass,
	}
}

// EvaluateCoalitionBundle computes U(B) for a coalition's current state and optionally executes the
// State Machine branch FORK to create a TensionTypeForkCandidate.
func (s *Store) EvaluateCoalitionBundle(ctx context.Context, workspaceID, coalitionID string, params BundleUtilityParams, alternativePatchRef string) (BundleDecision, error) {
	decision := CalculateBundleUtility(params)

	if decision.Decision == "FORK" {
		// Cluster automatically generates a FORK_CANDIDATE Tension
		if err := s.generateForkCandidateTension(ctx, workspaceID, coalitionID, alternativePatchRef, decision); err != nil {
			return decision, fmt.Errorf("failed to generate fork candidate: %w", err)
		}
	}

	return decision, nil
}

// generateForkCandidateTension executes the alternative branch when U(B) drops below MERGE threshold.
func (s *Store) generateForkCandidateTension(ctx context.Context, workspaceID, coalitionID, alternativePatchRef string, decision BundleDecision) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		taskIDs, primaryTaskID, primaryAgentID := s.bundleUtilityCoalitionTaskContextTx(ctx, tx, workspaceID, coalitionID)
		newTensionID := nextID("tens")
		title := fmt.Sprintf("Coalition Fork (%s/%s): Rejected Bundle with U(B)=%.3f", decision.NextAction, decision.ConflictSafeClass, decision.UtilityScore)
		body := fmt.Sprintf("The coalition %s produced conflicting patches. Next action: %s. Conflict-safe branch: %s. Rebase admission: %s. Rebase plan: %s. Decision: %s.\nAlternative Patch Ref: %s.", coalitionID, decision.NextAction, decision.ConflictSafeClass, decision.RebaseReason, decision.RebasePlanClass, decision.Reasoning, alternativePatchRef)

		artifactRefs := bundleUtilityForkArtifactRefs(alternativePatchRef)
		constraintRefs := bundleUtilityForkConstraintRefs(decision)
		evidence := bundleUtilityForkEvidenceRecords(newTensionID, workspaceID, artifactRefs, constraintRefs, now)
		evidenceRefs := evidenceRefsFromEvidence(evidence)
		score := bundleUtilityDecisionScore(decision.UtilityScore)
		record := TensionRecord{
			TensionID:       newTensionID,
			WorkspaceID:     workspaceID,
			ProtoClusterID:  bundleUtilityProtoClusterID(workspaceID, "", taskIDs),
			TensionType:     normalizeTensionType("fork_candidate"),
			LifecycleState:  tensionLifecycleEmergent,
			ReviewStatus:    tensionReviewPending,
			Title:           title,
			Summary:         body,
			AnchorKind:      "coalition_id",
			AnchorRef:       strings.TrimSpace(coalitionID),
			TaskIDs:         uniqueSortedStrings(taskIDs),
			AgentIDs:        uniqueSortedStrings([]string{primaryAgentID}),
			ArtifactRefs:    artifactRefs,
			ConstraintRefs:  constraintRefs,
			BaseScore:       score,
			SurfaceScore:    score,
			EvidenceCount:   len(evidence),
			LastSeenEventID: lastGovernedTensionEvidenceRef(evidenceRefs),
			LastSeenAt:      now,
			LastDetectedAt:  now,
			LastRefreshedAt: now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}

		if err := s.upsertTensionTx(ctx, tx, record); err != nil {
			return err
		}
		if err := s.upsertTensionEvidenceTx(ctx, tx, workspaceID, record.TensionID, evidence); err != nil {
			return err
		}

		// Create runtime event for control plane visibility.
		payload := tensionRuntimeEventPayload(record, evidenceRefs, "coalition_fork_generated", decision.Reasoning)
		payload["coalition_id"] = strings.TrimSpace(coalitionID)
		payload["u_b"] = decision.UtilityScore
		payload["decision_reason"] = decision.DecisionReason
		payload["decision_detail"] = decision.DecisionDetail
		payload["conflict_safe_class"] = decision.ConflictSafeClass
		payload["rebase_admissible"] = decision.RebaseAdmissible
		payload["rebase_reason"] = decision.RebaseReason
		payload["rebase_plan_class"] = decision.RebasePlanClass
		payload["next_action"] = decision.NextAction
		if strings.TrimSpace(primaryTaskID) != "" {
			payload["task_id"] = strings.TrimSpace(primaryTaskID)
			payload["task_ids"] = uniqueSortedStrings(taskIDs)
		}
		parentEvent, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "coalition_fork_generated",
			EntityType:  "tension",
			EntityID:    newTensionID,
			ActorType:   "system",
			ActorID:     "control_plane",
			TaskID:      strings.TrimSpace(primaryTaskID),
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		if err := s.generateRebaseFollowupTensionTx(ctx, tx, authority, workspaceID, coalitionID, alternativePatchRef, taskIDs, record, parentEvent, decision, now); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
