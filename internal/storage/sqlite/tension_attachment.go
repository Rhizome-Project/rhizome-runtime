package sqlite

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
)

// AgentAttachmentProfile is a placeholder for future vector embeddings.
// For now, Fit and Novelty are calculated heuristically.
type AgentAttachmentProfile struct {
	AgentID string
	// CapabilityEmbedding []float32
	// RecentIntent []float32
}

type AttachmentNoveltySignals struct {
	OccupierCount        int
	CrowdingRatio        float64
	RoleDiversity        float64
	EvidenceSignal       float64
	HasFarReviewer       bool
	GeneratorDistance    float64
	HasGeneratorEvidence bool
}

var (
	// Mathematical hyperparameters for Softmax attachment (from RRP-1.2 specification)
	ParamAlphaAttach   = 10.0 // Tension surface priority weight
	ParamBetaAttach    = 5.0  // Fit weight
	ParamGammaAttach   = 3.0  // Novelty weight
	ParamDeltaAttach   = 8.0  // Crowding penalty weight
	ParamEpsilonAttach = 1.0  // Switch penalty weight
	ParamEtaAttach     = 1.0  // Context-loss penalty weight
	ParamZetaAttach    = 1.5  // Stay bonus weight
	ParamXiAttach      = 1.0  // Exploration prior weight
	ParamThetaAttach   = 0.8  // Recovery-risk weight
	ParamIotaAttach    = 0.8  // Archive-propensity penalty weight
	ParamKappaAttach   = 0.35 // Lease-sensitive bonus weight
	ParamOmicronAttach = 0.9  // Far-reviewer-gap relief weight
	ParamFitMinAttach  = 0.35
	ParamBaseTemp      = 1.0 // Default Softmax temperature
)

const (
	AttachmentDecisionAllowed  = "allowed"
	AttachmentDecisionGuarded  = "guarded"
	AttachmentDecisionRejected = "rejected"
)

type AttachmentDecision struct {
	State   string   `json:"state"`
	Reasons []string `json:"reasons,omitempty"`
}

var (
	// Mathematical hyperparameters for Synergy
	ParamLambdaGoal           = 5.0
	ParamLambdaComp           = 5.0
	ParamLambdaCoord          = 2.0
	ParamLambdaCoalitionPrior = 2.0
	ParamLambdaCoalitionLock  = 1.0
)

// CalculateAgentAttachScore calculates the raw attach score (logit) for an agent against a tension.
func CalculateAgentAttachScore(
	agentID string,
	tension TensionRecord,
	occupierIds []string,
) float64 {
	// 1. Surface Score Priority
	// piSurf is in [0, 1] range
	piSurf := float64(tension.SurfaceScore) / 100.0
	if piSurf < 0 {
		piSurf = 0
	} else if piSurf > 1 {
		piSurf = 1
	}

	factors := CalculateAgentAttachFactors(agentID, tension, occupierIds)
	return calculateAttachScoreWithFactors(piSurf, factors)
}

func CalculateAgentAttachFactors(agentID string, tension TensionRecord, occupierIds []string) AgentAttachmentFactors {
	crowdingRatio := crowdingRatioForAttachment(tension, occupierIds)
	return AgentAttachmentFactors{
		Fit:                   deterministicFitEstimate(agentID, tension.TensionID),
		Novelty:               calculateAttachmentNoveltyFromSignals(AttachmentNoveltySignals{OccupierCount: len(occupierIds), CrowdingRatio: crowdingRatio}),
		CrowdingRatio:         crowdingRatio,
		ArchivePropensity:     clampCoalitionSignal(tension.ArchivePropensity),
		RecoveryRisk:          clampCoalitionSignal(tension.RecoveryRisk),
		LeaseSensitive:        tension.LeaseSensitive,
		PersonalizationJitter: stableSignedJitter("attach:jitter", agentID, tension.TensionID, 0.3),
	}
}

// CalculateSoftmaxDistribution takes a slice of raw scores (logits) and returns the softmax probabilities.
func CalculateSoftmaxDistribution(scores []float64, temperature float64) []float64 {
	if len(scores) == 0 {
		return nil
	}

	probs := make([]float64, len(scores))
	if temperature <= 0 {
		temperature = ParamBaseTemp
	}

	// Max trick for numerical stability
	maxScore := scores[0]
	for _, s := range scores {
		if s > maxScore {
			maxScore = s
		}
	}

	sumExp := 0.0
	for i, s := range scores {
		probs[i] = math.Exp((s - maxScore) / temperature)
		sumExp += probs[i]
	}

	for i := range probs {
		probs[i] /= sumExp
	}

	return probs
}

// CalculateCoalitionSynergy estimates the synergy score of a coalition.
func CalculateCoalitionSynergy(memberCount int) float64 {
	if memberCount < 2 {
		return 0.0
	}

	// Heuristic formula for Synergy without embeddings:
	goal := 0.8
	comp := 0.6

	// Coordination cost = c0 + c1*N + c2*N^2
	coord := 0.5 + 0.5*float64(memberCount) + 0.1*float64(memberCount*memberCount)

	return CalculateCoalitionSynergyFromSignals(goal, comp, 0.0, coord, 1.0)
}

func CalculateCoalitionSynergyFromSignals(goal, comp, prior, coord, lockPenalty float64) float64 {
	goal = clampCoalitionSignal(goal)
	comp = clampCoalitionSignal(comp)
	prior = clampCoalitionSignal(prior)
	if coord < 0 {
		coord = 0
	}
	if lockPenalty < 1.0 {
		lockPenalty = 1.0
	}
	return ParamLambdaGoal*goal +
		ParamLambdaComp*comp +
		ParamLambdaCoalitionPrior*prior -
		ParamLambdaCoord*coord -
		ParamLambdaCoalitionLock*math.Log(lockPenalty)
}

func clampCoalitionSignal(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}

func calculateAttachScoreWithFactors(piSurf float64, factors AgentAttachmentFactors) float64 {
	score := ParamAlphaAttach * math.Sqrt(piSurf)
	score += ParamBetaAttach * factors.Fit
	if factors.Fit >= ParamFitMinAttach {
		score += ParamGammaAttach * factors.Novelty
	}
	score -= ParamDeltaAttach * factors.CrowdingRatio
	score -= ParamIotaAttach * clampCoalitionSignal(factors.ArchivePropensity)
	score += ParamThetaAttach * clampCoalitionSignal(factors.RecoveryRisk)
	if factors.LeaseSensitive {
		score += ParamKappaAttach
	}
	score += ParamOmicronAttach * clampCoalitionSignal(factors.FarReviewerRelief)
	score -= ParamEpsilonAttach * factors.SwitchPenalty
	score -= ParamEtaAttach * factors.ContextLossPenalty
	score += ParamZetaAttach * factors.StayBonus
	score += ParamXiAttach * factors.ExplorationPrior
	score += factors.PersonalizationJitter
	return score
}

func attachmentExplorationCandidateWeight(factors AgentAttachmentFactors) float64 {
	weight := clampCoalitionSignal(factors.ExplorationPrior)
	fitSignal := 0.0
	if fit := clampCoalitionSignal(factors.Fit); fit > ParamFitMinAttach {
		fitSignal = (fit - ParamFitMinAttach) / (1.0 - ParamFitMinAttach)
	}
	weight += 0.03 * clampCoalitionSignal(fitSignal)
	weight -= 0.25 * clampCoalitionSignal(factors.CrowdingRatio)
	weight -= 0.12 * clampCoalitionSignal(factors.ArchivePropensity)
	weight += 0.04 * clampCoalitionSignal(factors.RecoveryRisk)
	weight += 0.05 * clampCoalitionSignal(factors.FarReviewerRelief)
	weight += 0.04 * clampCoalitionSignal(factors.Novelty)
	weight += 0.03 * clampCoalitionSignal(factors.StayBonus)
	weight -= 0.03 * clampCoalitionSignal(factors.SwitchPenalty)
	weight -= 0.02 * clampCoalitionSignal(factors.ContextLossPenalty)
	if factors.LeaseSensitive {
		weight += 0.02
	}
	return weight
}

func evaluateAttachmentDecision(tension TensionRecord, factors AgentAttachmentFactors) AttachmentDecision {
	reasons := make([]string, 0, 4)
	rejected := false
	guarded := false

	fit := clampCoalitionSignal(factors.Fit)
	crowding := clampCoalitionSignal(factors.CrowdingRatio)
	stayBonus := clampCoalitionSignal(factors.StayBonus)
	switchPenalty := clampCoalitionSignal(factors.SwitchPenalty)
	contextLoss := clampCoalitionSignal(factors.ContextLossPenalty)
	farReviewerRelief := clampCoalitionSignal(factors.FarReviewerRelief)

	if attachmentHasDirectRequirementAnchors(tension) && fit < ParamFitMinAttach+0.08 {
		reasons = appendAttachmentDecisionReason(reasons, "low_fit_for_explicit_anchors")
		rejected = true
	}
	if crowding >= 0.92 && farReviewerRelief < 0.25 && stayBonus < 0.35 {
		reasons = appendAttachmentDecisionReason(reasons, "crowding_without_relief")
		rejected = true
	} else if crowding >= 0.67 {
		reasons = appendAttachmentDecisionReason(reasons, "high_crowding")
		guarded = true
	}
	if contextLoss >= 0.25 && fit < ParamFitMinAttach+0.12 && stayBonus < 0.35 {
		reasons = appendAttachmentDecisionReason(reasons, "high_context_loss")
		guarded = true
	}
	if switchPenalty >= 0.40 && fit < ParamFitMinAttach+0.12 && stayBonus < 0.35 {
		reasons = appendAttachmentDecisionReason(reasons, "high_switch_penalty")
		guarded = true
	}
	if factors.LeaseSensitive && (len(tension.ConstraintRefs) > 0 || len(tension.SegmentRefs) > 0 || clampCoalitionSignal(factors.RecoveryRisk) >= 0.5 || crowding >= 0.67) {
		reasons = appendAttachmentDecisionReason(reasons, "lease_sensitive_coordination")
		guarded = true
	}

	switch {
	case rejected:
		return AttachmentDecision{State: AttachmentDecisionRejected, Reasons: reasons}
	case guarded:
		return AttachmentDecision{State: AttachmentDecisionGuarded, Reasons: reasons}
	default:
		return AttachmentDecision{State: AttachmentDecisionAllowed}
	}
}

func attachmentDecisionPriority(state string) int {
	switch state {
	case AttachmentDecisionAllowed:
		return 0
	case AttachmentDecisionGuarded:
		return 1
	case AttachmentDecisionRejected:
		return 2
	default:
		return 3
	}
}

func appendAttachmentDecisionReason(reasons []string, reason string) []string {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

func attachmentHasDirectRequirementAnchors(tension TensionRecord) bool {
	return len(tension.AgentIDs) > 0 || len(tension.SessionIDs) > 0
}

func crowdingRatioForAttachment(tension TensionRecord, occupierIds []string) float64 {
	maxCapacity := float64(coalitionSizeCapForTensionType(tension.TensionType))
	if maxCapacity <= 0 {
		maxCapacity = 3.0
	}
	crowd := float64(len(occupierIds)) / maxCapacity
	if crowd > 1.0 {
		crowd = 1.0
	}
	return crowd
}

func heuristicNoveltyFromOccupiers(occupierIds []string) float64 {
	return calculateAttachmentNoveltyFromSignals(AttachmentNoveltySignals{OccupierCount: len(occupierIds)})
}

func calculateAttachmentNoveltyFromSignals(signals AttachmentNoveltySignals) float64 {
	crowdingRatio := clampCoalitionSignal(signals.CrowdingRatio)
	novelty := 0.28 + 0.22*(1.0-crowdingRatio)
	if signals.OccupierCount == 0 {
		novelty += 0.18
	}
	novelty += 0.12 * clampCoalitionSignal(signals.RoleDiversity)
	novelty += 0.10 * clampCoalitionSignal(signals.EvidenceSignal)
	if signals.HasFarReviewer {
		novelty += 0.05
	}
	if signals.HasGeneratorEvidence && signals.GeneratorDistance > 0.6 {
		novelty += 0.20 * clampCoalitionSignal(signals.GeneratorDistance)
	}
	return clampCoalitionSignal(novelty)
}

func attachmentFarReviewerRelief(signals AttachmentNoveltySignals) float64 {
	if signals.OccupierCount == 0 || signals.HasFarReviewer {
		return 0
	}
	if !signals.HasGeneratorEvidence || signals.GeneratorDistance <= 0.6 {
		return 0
	}

	relief := 0.20
	relief += 0.35 * clampCoalitionSignal(signals.GeneratorDistance)
	relief += 0.15 * (1.0 - clampCoalitionSignal(signals.RoleDiversity))
	if signals.OccupierCount >= 2 {
		relief += 0.15
	}
	if signals.OccupierCount >= 3 {
		relief += 0.05
	}
	return clampCoalitionSignal(relief)
}

func deterministicFitEstimate(agentID, tensionID string) float64 {
	return ParamFitMinAttach + 0.4*stableUnitFloat("attach:fit", agentID, tensionID)
}

func stableUnitFloat(parts ...string) float64 {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	value := binary.BigEndian.Uint64(sum[:8])
	return float64(value) / float64(^uint64(0))
}

func stableSignedJitter(scope, agentID, tensionID string, amplitude float64) float64 {
	if amplitude <= 0 {
		return 0
	}
	return (stableUnitFloat(scope, agentID, tensionID) - 0.5) * 2 * amplitude
}
