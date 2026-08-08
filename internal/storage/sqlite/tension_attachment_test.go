package sqlite

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// Unit tests for the mathematical models
func TestCalculateAgentAttachScore(t *testing.T) {
	tension := TensionRecord{
		TensionID:    "ten-test-1",
		TensionType:  "bottleneck",
		SurfaceScore: 80, // piSurf = 0.8
	}
	agentID := "agent-x"

	// 1. No occupiers
	occupiers0 := []string{}
	score0 := CalculateAgentAttachScore(agentID, tension, occupiers0)

	// 2. Max capacity occupiers
	occupiersMax := []string{"a1", "a2", "a3", "a4"}
	scoreMax := CalculateAgentAttachScore(agentID, tension, occupiersMax)

	// Score with maximum occupiers should be much lower due to the crowding penalty and novelty drop
	if scoreMax >= score0 {
		t.Errorf("Expected crowded score (%f) to be less than empty score (%f)", scoreMax, score0)
	}
}

func TestCalculateAgentAttachFactorsKeepsEmptyCoalitionNoveltyModest(t *testing.T) {
	tension := TensionRecord{
		TensionID:   "ten-test-novelty",
		TensionType: "gap",
	}

	empty := CalculateAgentAttachFactors("agent-empty", tension, nil)
	occupied := CalculateAgentAttachFactors("agent-empty", tension, []string{"agent-a"})

	if empty.Novelty >= 1.0 {
		t.Fatalf("expected empty coalition novelty to stay below max certainty, got %+v", empty)
	}
	if empty.Novelty <= occupied.Novelty {
		t.Fatalf("expected empty coalition novelty to stay above occupied placeholder novelty, empty=%+v occupied=%+v", empty, occupied)
	}
}

func TestCalculateAttachScoreWithFactorsIncludesAdvisoryPressure(t *testing.T) {
	base := AgentAttachmentFactors{
		Fit:           0.55,
		Novelty:       0.68,
		CrowdingRatio: 0,
	}

	archival := calculateAttachScoreWithFactors(0.82, AgentAttachmentFactors{
		Fit:               base.Fit,
		Novelty:           base.Novelty,
		CrowdingRatio:     base.CrowdingRatio,
		ArchivePropensity: 0.75,
		RecoveryRisk:      0.10,
	})
	recoverable := calculateAttachScoreWithFactors(0.82, AgentAttachmentFactors{
		Fit:               base.Fit,
		Novelty:           base.Novelty,
		CrowdingRatio:     base.CrowdingRatio,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.55,
		LeaseSensitive:    true,
	})

	if recoverable <= archival {
		t.Fatalf("expected recovery/lease pressure to outrank archival pressure under matched fit, archival=%f recoverable=%f", archival, recoverable)
	}
}

func TestCalculateAttachScoreWithFactorsRewardsFarReviewerRelief(t *testing.T) {
	base := calculateAttachScoreWithFactors(0.82, AgentAttachmentFactors{
		Fit:                   0.61,
		Novelty:               0.58,
		CrowdingRatio:         0.66,
		PersonalizationJitter: 0,
	})
	relieved := calculateAttachScoreWithFactors(0.82, AgentAttachmentFactors{
		Fit:                   0.61,
		Novelty:               0.58,
		CrowdingRatio:         0.66,
		FarReviewerRelief:     0.62,
		PersonalizationJitter: 0,
	})

	if relieved <= base {
		t.Fatalf("expected far-reviewer gap relief to improve attach score, base=%f relieved=%f", base, relieved)
	}
}

func TestHeuristicAttachmentFitTracksRequirementCoverage(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}

	matched := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-fit"},
		TaskIDs:      []string{"task-fit"},
		SessionIDs:   []string{"sess-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, nil, currentSession)

	partial := heuristicAttachmentFit("agent-fit", TensionRecord{
		DocKeys:      []string{"doc-fit-a", "doc-other"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://other"},
	}, nil, currentSession)

	generic := heuristicAttachmentFit("agent-fit", TensionRecord{}, nil, currentSession)

	mismatched := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-other"},
		TaskIDs:      []string{"task-other"},
		SessionIDs:   []string{"sess-other"},
		DocKeys:      []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs: []string{"artifact://other-a", "artifact://other-b"},
	}, nil, currentSession)

	if matched <= generic {
		t.Fatalf("expected full requirement coverage to outrank generic attach fit, matched=%f generic=%f", matched, generic)
	}
	if matched <= partial {
		t.Fatalf("expected full requirement coverage to outrank partial overlap, matched=%f partial=%f", matched, partial)
	}
	if partial <= generic {
		t.Fatalf("expected partial overlap to outrank generic attach fit, partial=%f generic=%f", partial, generic)
	}
	if generic <= mismatched {
		t.Fatalf("expected generic attach fit to outrank explicit mismatch, generic=%f mismatched=%f", generic, mismatched)
	}
}

func TestHeuristicAttachmentFitDoesNotTreatExplicitAgentSessionTargetsAsGeneric(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID: "sess-fit",
		TaskID:    "task-fit",
	}

	generic := heuristicAttachmentFit("agent-fit", TensionRecord{}, nil, currentSession)

	exactAgent := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs: []string{"agent-fit"},
	}, nil, currentSession)
	exactSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		SessionIDs: []string{"sess-fit"},
	}, nil, currentSession)
	exactBoth := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:   []string{"agent-fit"},
		SessionIDs: []string{"sess-fit"},
	}, nil, currentSession)

	mismatchAgent := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs: []string{"agent-other"},
	}, nil, currentSession)
	mismatchSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		SessionIDs: []string{"sess-other"},
	}, nil, currentSession)
	mismatchBoth := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:   []string{"agent-other"},
		SessionIDs: []string{"sess-other"},
	}, nil, currentSession)

	if generic <= mismatchAgent || generic <= mismatchSession || generic <= mismatchBoth {
		t.Fatalf("expected explicit agent/session mismatches without richer anchors to stay below generic attach fit, generic=%f mismatchAgent=%f mismatchSession=%f mismatchBoth=%f", generic, mismatchAgent, mismatchSession, mismatchBoth)
	}
	if exactAgent <= mismatchAgent {
		t.Fatalf("expected explicit agent target match to outrank explicit agent mismatch, exact=%f mismatch=%f", exactAgent, mismatchAgent)
	}
	if exactSession <= mismatchSession {
		t.Fatalf("expected explicit session target match to outrank explicit session mismatch, exact=%f mismatch=%f", exactSession, mismatchSession)
	}
	if exactBoth <= mismatchBoth {
		t.Fatalf("expected combined explicit agent/session target match to outrank combined mismatch, exact=%f mismatch=%f", exactBoth, mismatchBoth)
	}
}

func TestHeuristicAttachmentFitCountsExplicitAgentSessionTargetsInsideStructuredCoverage(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}

	exact := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-fit"},
		SessionIDs:   []string{"sess-fit"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	genericStructured := heuristicAttachmentFit("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	mismatch := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-other"},
		SessionIDs:   []string{"sess-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	if exact <= genericStructured {
		t.Fatalf("expected exact explicit agent/session targets to raise structured requirement fit beyond generic structured coverage, exact=%f generic=%f", exact, genericStructured)
	}
	if genericStructured <= mismatch {
		t.Fatalf("expected explicit agent/session mismatch to reduce structured requirement fit below generic structured coverage, generic=%f mismatch=%f", genericStructured, mismatch)
	}
}

func TestHeuristicAttachmentFitScalesStructuredSessionTargetsByRetainedContinuity(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}

	exact := heuristicAttachmentFit("agent-fit", TensionRecord{
		SessionIDs:   []string{"sess-fit"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	genericStructured := heuristicAttachmentFit("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	retained := heuristicAttachmentFit("agent-fit", TensionRecord{
		SessionIDs:   []string{"sess-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	mismatch := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-other"},
		SessionIDs:   []string{"sess-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	if exact <= genericStructured {
		t.Fatalf("expected exact structured session match to outrank generic structured coverage, exact=%f generic=%f", exact, genericStructured)
	}
	if genericStructured <= retained {
		t.Fatalf("expected generic structured coverage to stay above retained non-exact session targeting, generic=%f retained=%f", genericStructured, retained)
	}
	if retained <= mismatch {
		t.Fatalf("expected retained continuity to lift structured non-exact session targeting above explicit agent/session mismatch, retained=%f mismatch=%f", retained, mismatch)
	}
}

func TestHeuristicAttachmentFitPreservesStructuredAgentTargetSignalWithoutSessionContext(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}

	exactWithSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-fit"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	exactNoSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-fit"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, nil)

	genericNoSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, nil)

	mismatchNoSession := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, nil)

	if exactNoSession <= genericNoSession {
		t.Fatalf("expected structured explicit agent target to remain visible without session context, exactNoSession=%f genericNoSession=%f", exactNoSession, genericNoSession)
	}
	if exactNoSession <= mismatchNoSession {
		t.Fatalf("expected structured explicit agent target to outrank explicit agent mismatch without session context, exactNoSession=%f mismatchNoSession=%f", exactNoSession, mismatchNoSession)
	}
	if exactWithSession <= exactNoSession {
		t.Fatalf("expected active session context to keep stronger structured agent target credit than no-session fallback, exactWithSession=%f exactNoSession=%f", exactWithSession, exactNoSession)
	}
}

func TestAttachmentSessionAnchorRetentionScalesDocArtifactOverlapDepth(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}

	full := attachmentSessionAnchorRetention(TensionRecord{
		SessionIDs:   []string{"sess-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	partial := attachmentSessionAnchorRetention(TensionRecord{
		DocKeys:      []string{"doc-fit-a", "doc-other"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://other"},
	}, currentSession)

	thin := attachmentSessionAnchorRetention(TensionRecord{
		DocKeys: []string{"doc-fit-a", "doc-other"},
	}, currentSession)

	none := attachmentSessionAnchorRetention(TensionRecord{
		SessionIDs:   []string{"sess-other"},
		DocKeys:      []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs: []string{"artifact://other-a", "artifact://other-b"},
	}, currentSession)

	if math.Abs(full-1.0) > 1e-9 {
		t.Fatalf("expected full anchor retention to saturate at 1.0, got %f", full)
	}
	if partial <= thin {
		t.Fatalf("expected doc+artifact overlap to retain more continuity than doc-only overlap, partial=%f thin=%f", partial, thin)
	}
	if thin <= none {
		t.Fatalf("expected thin overlap to retain more continuity than none, thin=%f none=%f", thin, none)
	}
}

func TestAttachmentAgentAffinityBonusScalesStructuredExplicitTargets(t *testing.T) {
	unstructuredExact := attachmentAgentAffinityBonus("agent-fit", TensionRecord{
		AgentIDs: []string{"agent-fit"},
	})

	structuredExact := attachmentAgentAffinityBonus("agent-fit", TensionRecord{
		AgentIDs: []string{"agent-fit"},
		TaskIDs:  []string{"task-fit"},
	})

	structuredMismatch := attachmentAgentAffinityBonus("agent-fit", TensionRecord{
		AgentIDs: []string{"agent-other"},
		TaskIDs:  []string{"task-fit"},
	})

	if math.Abs(unstructuredExact-0.12) > 1e-9 {
		t.Fatalf("expected unstructured explicit agent target to keep full affinity bonus, got %f", unstructuredExact)
	}
	if math.Abs(structuredExact-0.06) > 1e-9 {
		t.Fatalf("expected structured explicit agent target to keep only bounded partial affinity bonus on top of requirement coverage, got %f", structuredExact)
	}
	if structuredMismatch != 0 {
		t.Fatalf("expected structured explicit agent mismatch to keep zero affinity bonus, got %f", structuredMismatch)
	}
}

func TestAttachmentStayBonusScalesSessionAnchorRetention(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}

	full := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-switch"},
		SessionIDs:   []string{"sess-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, nil, currentSession)

	partial := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-switch"},
		DocKeys:      []string{"doc-fit-a", "doc-other"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://other"},
	}, nil, currentSession)

	thin := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs: []string{"task-switch"},
		DocKeys: []string{"doc-fit-a", "doc-other"},
	}, nil, currentSession)

	none := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-switch"},
		SessionIDs:   []string{"sess-other"},
		DocKeys:      []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs: []string{"artifact://other-a", "artifact://other-b"},
	}, nil, currentSession)

	currentTask := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs: []string{"task-fit"},
	}, nil, currentSession)

	occupied := attachmentStayBonus("agent-fit", TensionRecord{
		TaskIDs: []string{"task-switch"},
	}, []string{"agent-fit"}, currentSession)

	if occupied != 1.0 {
		t.Fatalf("expected occupied tension to keep max stay bonus, got %f", occupied)
	}
	if currentTask <= full {
		t.Fatalf("expected current-task stay bonus to outrank off-task retained continuity, current=%f full=%f", currentTask, full)
	}
	if full <= partial {
		t.Fatalf("expected full off-task continuity to outrank partial retention, full=%f partial=%f", full, partial)
	}
	if partial <= thin {
		t.Fatalf("expected partial off-task continuity to outrank thin retention, partial=%f thin=%f", partial, thin)
	}
	if thin <= none {
		t.Fatalf("expected thin off-task continuity to outrank none, thin=%f none=%f", thin, none)
	}
}

func TestAttachmentSessionAffinityBonusScalesRetainedContinuity(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}

	exact := attachmentSessionAffinityBonus(TensionRecord{
		SessionIDs: []string{"sess-fit"},
	}, currentSession)

	full := attachmentSessionAffinityBonus(TensionRecord{
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	structuredRetained := attachmentSessionAffinityBonus(TensionRecord{
		SessionIDs:   []string{"sess-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	partial := attachmentSessionAffinityBonus(TensionRecord{
		DocKeys:      []string{"doc-fit-a", "doc-other"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://other"},
	}, currentSession)

	thin := attachmentSessionAffinityBonus(TensionRecord{
		DocKeys: []string{"doc-fit-a", "doc-other"},
	}, currentSession)

	none := attachmentSessionAffinityBonus(TensionRecord{
		SessionIDs:   []string{"sess-other"},
		DocKeys:      []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs: []string{"artifact://other-a", "artifact://other-b"},
	}, currentSession)

	if math.Abs(exact-0.08) > 1e-9 {
		t.Fatalf("expected exact session affinity to stay fixed, got %f", exact)
	}
	if structuredRetained != 0 {
		t.Fatalf("expected structured non-exact session targeting to leave retained continuity credit to requirement coverage instead of stacking session affinity fallback, got %f", structuredRetained)
	}
	if exact <= full {
		t.Fatalf("expected exact session match to outrank retained continuity bonus, exact=%f full=%f", exact, full)
	}
	if full <= partial {
		t.Fatalf("expected full retained continuity to outrank partial continuity bonus, full=%f partial=%f", full, partial)
	}
	if partial <= thin {
		t.Fatalf("expected partial retained continuity to outrank thin continuity bonus, partial=%f thin=%f", partial, thin)
	}
	if thin <= none {
		t.Fatalf("expected thin retained continuity to outrank none, thin=%f none=%f", thin, none)
	}
}

func TestHeuristicAttachmentFitScalesStructuredAgentTargetsAboveGenericCoverage(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-fit",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}

	exactAgent := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-fit"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	genericStructured := heuristicAttachmentFit("agent-fit", TensionRecord{
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	mismatch := heuristicAttachmentFit("agent-fit", TensionRecord{
		AgentIDs:     []string{"agent-other"},
		TaskIDs:      []string{"task-fit"},
		DocKeys:      []string{"doc-fit"},
		ArtifactRefs: []string{"artifact://fit"},
	}, nil, currentSession)

	if exactAgent <= genericStructured {
		t.Fatalf("expected structured explicit agent target to keep bounded fit uplift above generic structured coverage, exact=%f generic=%f", exactAgent, genericStructured)
	}
	if genericStructured <= mismatch {
		t.Fatalf("expected generic structured coverage to stay above explicit structured agent mismatch, generic=%f mismatch=%f", genericStructured, mismatch)
	}
}

func TestAttachmentExplorationPriorDampensRetainedContinuity(t *testing.T) {
	currentSession := &AgentSessionStateRecord{
		SessionID:      "sess-fit",
		TaskID:         "task-current",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}

	cold := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-other"},
		DocKeys:      []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs: []string{"artifact://other-a", "artifact://other-b"},
	}, currentSession)

	partial := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-other"},
		DocKeys:      []string{"doc-fit-a", "doc-other"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://other"},
	}, currentSession)

	targetedAgent := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-other"},
		AgentIDs:     []string{"agent-rookie"},
	}, currentSession)

	targetedSession := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-other"},
		SessionIDs:   []string{"sess-other"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	full := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-other"},
		SessionIDs:   []string{"sess-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	currentTask := attachmentExplorationPrior(0.8, TensionRecord{
		TensionType:  "gap",
		SurfaceScore: 20,
		TaskIDs:      []string{"task-current"},
		SessionIDs:   []string{"sess-fit"},
		DocKeys:      []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs: []string{"artifact://fit-a", "artifact://fit-b"},
	}, currentSession)

	if math.Abs(cold-currentTask) > 1e-9 {
		t.Fatalf("expected current-task path to keep full exploration prior, cold=%f currentTask=%f", cold, currentTask)
	}
	if cold <= partial {
		t.Fatalf("expected cold off-task path to keep stronger exploration prior than partial retained continuity, cold=%f partial=%f", cold, partial)
	}
	if targetedAgent != 0 || targetedSession != 0 {
		t.Fatalf("expected explicit off-task agent/session targeting to suppress exploration prior, targetedAgent=%f targetedSession=%f", targetedAgent, targetedSession)
	}
	if partial <= full {
		t.Fatalf("expected partial retained continuity without explicit session targeting to keep more exploration prior than explicit targeted path, partial=%f full=%f", partial, full)
	}
	if full != 0 {
		t.Fatalf("expected explicit off-task session targeting to suppress exploration prior instead of merely damping it, got %f", full)
	}
}

func TestAttachmentExplorationCandidateWeightReusesAdvisoryPressure(t *testing.T) {
	archival := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.70,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.80,
		RecoveryRisk:      0.05,
	})
	recoverable := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.66,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.60,
		LeaseSensitive:    true,
	})

	if recoverable <= archival {
		t.Fatalf("expected exploration candidate weight to reuse recovery/lease pressure over archival pressure, archival=%f recoverable=%f", archival, recoverable)
	}
}

func TestAttachmentExplorationCandidateWeightReusesFarReviewerRelief(t *testing.T) {
	covered := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.62,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
	})
	openGap := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.58,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
		FarReviewerRelief: 0.90,
	})

	if openGap <= covered {
		t.Fatalf("expected exploration candidate weight to reuse far-reviewer relief over a covered sibling, covered=%f openGap=%f", covered, openGap)
	}
}

func TestAttachmentExplorationCandidateWeightReusesNovelty(t *testing.T) {
	stale := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.62,
		Novelty:           0.20,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
	})
	fresh := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.60,
		Novelty:           0.90,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
	})

	if fresh <= stale {
		t.Fatalf("expected exploration candidate weight to reuse surfaced novelty over a staler sibling, stale=%f fresh=%f", stale, fresh)
	}
}

func TestAttachmentExplorationCandidateWeightReusesFit(t *testing.T) {
	barelyAttachable := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.62,
		Fit:               0.30,
		Novelty:           0.80,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
	})
	viable := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:  0.60,
		Fit:               0.90,
		Novelty:           0.80,
		CrowdingRatio:     0.10,
		ArchivePropensity: 0.10,
		RecoveryRisk:      0.10,
	})

	if viable <= barelyAttachable {
		t.Fatalf("expected exploration candidate weight to reuse bounded attach fit over a barely attachable sibling, barelyAttachable=%f viable=%f", barelyAttachable, viable)
	}
}

func TestAttachmentExplorationCandidateWeightReusesInertiaRelief(t *testing.T) {
	highInertia := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:   0.62,
		Novelty:            0.80,
		Fit:                0.85,
		CrowdingRatio:      0.10,
		ArchivePropensity:  0.10,
		RecoveryRisk:       0.10,
		StayBonus:          0.00,
		SwitchPenalty:      0.60,
		ContextLossPenalty: 0.60,
	})
	retained := attachmentExplorationCandidateWeight(AgentAttachmentFactors{
		ExplorationPrior:   0.60,
		Novelty:            0.80,
		Fit:                0.85,
		CrowdingRatio:      0.10,
		ArchivePropensity:  0.10,
		RecoveryRisk:       0.10,
		StayBonus:          0.30,
		SwitchPenalty:      0.10,
		ContextLossPenalty: 0.10,
	})

	if retained <= highInertia {
		t.Fatalf("expected exploration candidate weight to reuse bounded inertia relief over a high-inertia sibling, highInertia=%f retained=%f", highInertia, retained)
	}
}

func TestCalculateSoftmaxDistribution(t *testing.T) {
	scores := []float64{10.0, 5.0, 2.0}
	probs := CalculateSoftmaxDistribution(scores, 1.0)

	if len(probs) != 3 {
		t.Fatalf("Expected 3 probabilities, got %d", len(probs))
	}

	sum := 0.0
	for _, p := range probs {
		if p < 0 || p > 1 {
			t.Errorf("Probability %f out of bounds", p)
		}
		sum += p
	}

	if math.Abs(sum-1.0) > 1e-6 {
		t.Errorf("Probabilities do not sum to 1. Got: %f", sum)
	}

	// 10.0 should have a much higher probability than 5.0
	if probs[0] <= probs[1] {
		t.Errorf("Expected higher score to have higher probability. P(10.0) = %f, P(5.0) = %f", probs[0], probs[1])
	}
}

func TestCalculateCoalitionSynergy(t *testing.T) {
	syn0 := CalculateCoalitionSynergy(0)
	syn1 := CalculateCoalitionSynergy(1)
	if syn0 != 0 || syn1 != 0 {
		t.Errorf("Expected 0 synergy for <2 members. Got %f, %f", syn0, syn1)
	}

	syn2 := CalculateCoalitionSynergy(2)
	syn3 := CalculateCoalitionSynergy(3)

	if syn2 <= 0 {
		t.Errorf("Expected positive synergy for 2 members. Got %f", syn2)
	}

	// With increasing members, coordination cost rises. Check differences if needed.
	_ = syn3
}

// Integration tests for Data Layer
func TestCoalitionAttachment(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-test"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-c1", "agent-c2", "agent-c3")
	ensureTensionOverlayTables(t, ctx, store)

	t.Run("Create and Add Member", func(t *testing.T) {
		agent1 := "agent-c1"
		agent2 := "agent-c2"
		tensionID := "tension:attach:test1"
		now := time.Now().UTC().Format(time.RFC3339Nano)

		// Mock a tension record physically so foreign keys don't fail, though if FK are off it might be fine.
		// assure tension is there
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      tensionID,
			TensionType:    "bottleneck",
			LifecycleState: tensionLifecycleActive,
			CreatedAt:      now,
			UpdatedAt:      now,
		})

		coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Fix bugs")
		if err != nil {
			t.Fatalf("Failed to create coalition: %v", err)
		}
		if coalition.Status != "FORMING" {
			t.Errorf("Expected FORMING status, got %s", coalition.Status)
		}

		err = store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agent1, 0.8, 1.0)
		if err != nil {
			t.Fatalf("Failed to add member 1: %v", err)
		}

		err = store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agent2, 0.7, 0.5)
		if err != nil {
			t.Fatalf("Failed to add member 2: %v", err)
		}

		c2, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
		if err != nil {
			t.Fatalf("Failed to get coalition: %v", err)
		}

		if len(c2.Members) != 2 {
			t.Errorf("Expected 2 members, got %d", len(c2.Members))
		}

		if c2.Status != "ACTIVE" {
			t.Errorf("Expected ACTIVE status for 2 members, got %s", c2.Status)
		}
		if c2.SynergyScore <= 0 {
			t.Errorf("Expected positive synergy score, got %f", c2.SynergyScore)
		}

		// Test list scored tensions
		t.Run("ListAgentAvailableTensionsScored", func(t *testing.T) {
			scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-c3")
			if err != nil {
				t.Fatalf("List scored tensions failed: %v", err)
			}
			if len(scored) == 0 {
				t.Fatalf("Expected at least 1 scored tension")
			}
			// Probability should be set
			if scored[0].AttachProb <= 0 {
				t.Errorf("Expected attach probability > 0, got %f", scored[0].AttachProb)
			}
		})

		// Detach is locked until the minimum tenure epoch has elapsed.
		err = store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agent1)
		if !errors.Is(err, ErrCoalitionMinimumTenureNotMet) {
			t.Fatalf("expected tenure lock before epoch advance, got %v", err)
		}

		if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
			t.Fatalf("Failed to increment epoch: %v", err)
		}

		err = store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agent1)
		if err != nil {
			t.Fatalf("Failed to remove member: %v", err)
		}

		c3, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
		if err != nil {
			t.Fatalf("Failed to get coalition: %v", err)
		}
		if len(c3.Members) != 1 {
			t.Errorf("Expected 1 member after removal, got %d", len(c3.Members))
		}
	})
}

func TestListAgentAvailableTensionsScoredPersonalizesTieOrderAcrossAgents(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-personalized-order"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-p1", "agent-p2")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, suffix := range []string{"a", "b", "c", "d"} {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:personalized:" + suffix,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			BaseScore:      60,
			SurfaceScore:   80,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	left, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-p1")
	if err != nil {
		t.Fatalf("list scored tensions for agent-p1: %v", err)
	}
	right, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-p2")
	if err != nil {
		t.Fatalf("list scored tensions for agent-p2: %v", err)
	}
	if len(left) != 4 || len(right) != 4 {
		t.Fatalf("expected four scored tensions, got left=%d right=%d", len(left), len(right))
	}

	leftOrder := joinScoredTensionIDs(left)
	rightOrder := joinScoredTensionIDs(right)
	if leftOrder == rightOrder {
		t.Fatalf("expected per-agent personalization jitter to avoid identical shortlist order, got %s", leftOrder)
	}
}

func TestListAgentAvailableTensionsScoredPrefersCurrentTaskContext(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-current-context"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-stay")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-stay", "node-stay")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-switch", "node-switch")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-stay", "agent-stay")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-stay",
		AgentID:     "agent-stay",
		TaskID:      "task-stay",
		Summary:     "agent is already attached to task-stay",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:stay",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-stay"},
		BaseScore:      60,
		SurfaceScore:   80,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:switch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-switch"},
		BaseScore:      60,
		SurfaceScore:   80,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-stay")
	if err != nil {
		t.Fatalf("list scored tensions with current context: %v", err)
	}
	if len(scored) != 2 {
		t.Fatalf("expected two scored tensions, got %+v", scored)
	}
	if scored[0].TensionID != "tension:stay" {
		t.Fatalf("expected current-task tension to rank first, got %+v", scored)
	}
	if scored[0].AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected stay bonus on current-task tension, got %+v", scored[0].AttachFactors)
	}
	if scored[1].AttachFactors.SwitchPenalty <= 0 || scored[1].AttachFactors.ContextLossPenalty <= 0 {
		t.Fatalf("expected switch/context penalties on non-current tension, got %+v", scored[1].AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredPrefersSessionAlignedRequirementCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-session-fit"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:      model.SessionEventStart,
		WorkspaceID:    workspaceID,
		SessionID:      "sess-fit",
		AgentID:        "agent-fit",
		TaskID:         "task-fit",
		Summary:        "agent is already attached to task-fit",
		OwnerScope:     "task/session",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:session:match",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		SessionIDs:     []string{"sess-fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:session:mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-other"},
		SessionIDs:     []string{"sess-other"},
		DocKeys:        []string{"doc-other"},
		ArtifactRefs:   []string{"artifact://other"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:session:retained",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-other"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with session-aligned requirements: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	match := requireScoredTensionByID(t, scored, "tension:session:match")
	retained := requireScoredTensionByID(t, scored, "tension:session:retained")
	mismatch := requireScoredTensionByID(t, scored, "tension:session:mismatch")
	if match.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected session-aligned requirement coverage to surface stronger fit, match=%+v mismatch=%+v", match.AttachFactors, mismatch.AttachFactors)
	}
	if retained.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected retained continuity to outrank explicit mismatch, retained=%+v mismatch=%+v", retained.AttachFactors, mismatch.AttachFactors)
	}
	if retained.AttachFactors.StayBonus <= 0 || retained.AttachFactors.SwitchPenalty >= mismatch.AttachFactors.SwitchPenalty {
		t.Fatalf("expected retained continuity to surface bounded inertia relief, retained=%+v mismatch=%+v", retained.AttachFactors, mismatch.AttachFactors)
	}
	if scored[0].TensionID != "tension:session:match" {
		t.Fatalf("expected session-aligned tension to rank first, got %+v", scored)
	}
	if retained.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected retained continuity to improve shortlist rank beyond mismatch, retained=%+v mismatch=%+v", retained, mismatch)
	}
}

func TestListAgentAvailableTensionsScoredKeepsExplicitAgentSessionMismatchBelowGeneric(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-explicit-target-mismatch"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-fit",
		AgentID:     "agent-fit",
		TaskID:      "task-fit",
		Summary:     "agent is already attached to task-fit",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:generic",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:target:exact",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-fit"},
		SessionIDs:     []string{"sess-fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:target:mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-other"},
		SessionIDs:     []string{"sess-other"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with explicit agent/session targeting: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	generic := requireScoredTensionByID(t, scored, "tension:generic")
	exact := requireScoredTensionByID(t, scored, "tension:target:exact")
	mismatch := requireScoredTensionByID(t, scored, "tension:target:mismatch")

	if generic.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected generic tension to keep stronger fit than explicit agent/session mismatch without richer anchors, generic=%+v mismatch=%+v", generic.AttachFactors, mismatch.AttachFactors)
	}
	if exact.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected explicit agent/session target match to outrank explicit mismatch, exact=%+v mismatch=%+v", exact.AttachFactors, mismatch.AttachFactors)
	}
	if generic.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected generic tension to rank above explicit agent/session mismatch without richer anchors, generic=%+v mismatch=%+v", generic, mismatch)
	}
	if exact.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected exact explicit target to rank above explicit mismatch, exact=%+v mismatch=%+v", exact, mismatch)
	}
	if exact.AttachDecision.State != AttachmentDecisionAllowed {
		t.Fatalf("expected exact explicit target to remain attach-allowed, got %+v", exact.AttachDecision)
	}
	if generic.AttachDecision.State != AttachmentDecisionAllowed {
		t.Fatalf("expected generic tension to remain attach-allowed, got %+v", generic.AttachDecision)
	}
	if mismatch.AttachDecision.State != AttachmentDecisionRejected {
		t.Fatalf("expected explicit agent/session mismatch to be attach-rejected, got %+v", mismatch.AttachDecision)
	}
	if !containsString(mismatch.AttachDecision.Reasons, "low_fit_for_explicit_anchors") {
		t.Fatalf("expected explicit mismatch rejection to cite low fit for anchors, got %+v", mismatch.AttachDecision)
	}
}

func TestListAgentAvailableTensionsScoredUsesExplicitAgentSessionTargetsInsideStructuredCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-explicit-structured-targets"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:      model.SessionEventStart,
		WorkspaceID:    workspaceID,
		SessionID:      "sess-fit",
		AgentID:        "agent-fit",
		TaskID:         "task-fit",
		Summary:        "agent is already attached to task-fit",
		OwnerScope:     "task/session",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:generic",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:exact",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-fit"},
		SessionIDs:     []string{"sess-fit"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:retained",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		SessionIDs:     []string{"sess-other"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-other"},
		SessionIDs:     []string{"sess-other"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with structured explicit targets: %v", err)
	}
	if len(scored) != 4 {
		t.Fatalf("expected four scored tensions, got %+v", scored)
	}

	generic := requireScoredTensionByID(t, scored, "tension:structured:generic")
	exact := requireScoredTensionByID(t, scored, "tension:structured:exact")
	retained := requireScoredTensionByID(t, scored, "tension:structured:retained")
	mismatch := requireScoredTensionByID(t, scored, "tension:structured:mismatch")

	if exact.AttachFactors.Fit <= generic.AttachFactors.Fit {
		t.Fatalf("expected exact explicit targets to raise structured fit above generic structured coverage, exact=%+v generic=%+v", exact.AttachFactors, generic.AttachFactors)
	}
	if generic.AttachFactors.Fit <= retained.AttachFactors.Fit {
		t.Fatalf("expected generic structured coverage to stay above retained non-exact session targeting, generic=%+v retained=%+v", generic.AttachFactors, retained.AttachFactors)
	}
	if retained.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected retained continuity to lift non-exact structured session targeting above explicit mismatch, retained=%+v mismatch=%+v", retained.AttachFactors, mismatch.AttachFactors)
	}
	if generic.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected explicit agent/session mismatch to reduce structured fit below generic structured coverage, generic=%+v mismatch=%+v", generic.AttachFactors, mismatch.AttachFactors)
	}
	if exact.AttachProb <= generic.AttachProb {
		t.Fatalf("expected exact explicit targets to rank above generic structured coverage, exact=%+v generic=%+v", exact, generic)
	}
	if generic.AttachProb <= retained.AttachProb {
		t.Fatalf("expected generic structured coverage to rank above retained non-exact session targeting, generic=%+v retained=%+v", generic, retained)
	}
	if retained.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected retained continuity to rank above explicit mismatch under matched task/doc/artifact anchors, retained=%+v mismatch=%+v", retained, mismatch)
	}
	if generic.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected generic structured coverage to rank above explicit mismatch under matched task/doc/artifact anchors, generic=%+v mismatch=%+v", generic, mismatch)
	}
}

func TestListAgentAvailableTensionsScoredKeepsStructuredAgentTargetsAboveGenericCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-structured-agent-partial-bonus"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:      model.SessionEventStart,
		WorkspaceID:    workspaceID,
		SessionID:      "sess-fit",
		AgentID:        "agent-fit",
		TaskID:         "task-fit",
		Summary:        "agent is already attached to task-fit",
		OwnerScope:     "task/session",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:generic",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:agent-exact",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-fit"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:agent-mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-other"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with structured explicit agent targeting: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	generic := requireScoredTensionByID(t, scored, "tension:structured:generic")
	exact := requireScoredTensionByID(t, scored, "tension:structured:agent-exact")
	mismatch := requireScoredTensionByID(t, scored, "tension:structured:agent-mismatch")

	if exact.AttachFactors.Fit <= generic.AttachFactors.Fit {
		t.Fatalf("expected structured explicit agent target to keep bounded fit uplift above generic structured coverage, exact=%+v generic=%+v", exact.AttachFactors, generic.AttachFactors)
	}
	if generic.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected generic structured coverage to stay above explicit structured agent mismatch, generic=%+v mismatch=%+v", generic.AttachFactors, mismatch.AttachFactors)
	}
	if exact.AttachProb <= generic.AttachProb {
		t.Fatalf("expected structured explicit agent target to rank above generic structured coverage, exact=%+v generic=%+v", exact, generic)
	}
	if generic.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected generic structured coverage to rank above explicit structured agent mismatch, generic=%+v mismatch=%+v", generic, mismatch)
	}
}

func TestListAgentAvailableTensionsScoredPreservesStructuredAgentTargetsWithoutActiveSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-structured-agent-no-session"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:generic",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:agent-exact",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-fit"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:structured:agent-mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-other"},
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions without active session: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	generic := requireScoredTensionByID(t, scored, "tension:structured:generic")
	exact := requireScoredTensionByID(t, scored, "tension:structured:agent-exact")
	mismatch := requireScoredTensionByID(t, scored, "tension:structured:agent-mismatch")

	if exact.AttachFactors.Fit <= generic.AttachFactors.Fit {
		t.Fatalf("expected explicit structured agent target to improve fit without active session, exact=%+v generic=%+v", exact.AttachFactors, generic.AttachFactors)
	}
	if exact.AttachFactors.Fit <= mismatch.AttachFactors.Fit {
		t.Fatalf("expected explicit structured agent target to outrank mismatch without active session, exact=%+v mismatch=%+v", exact.AttachFactors, mismatch.AttachFactors)
	}
	if exact.AttachProb <= generic.AttachProb {
		t.Fatalf("expected explicit structured agent target to rank above generic structured coverage without active session, exact=%+v generic=%+v", exact, generic)
	}
	if exact.AttachProb <= mismatch.AttachProb {
		t.Fatalf("expected explicit structured agent target to rank above mismatch without active session, exact=%+v mismatch=%+v", exact, mismatch)
	}
}

func TestListAgentAvailableTensionsScoredPrefersRicherDocArtifactRequirementCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-graded-anchor-fit"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:      model.SessionEventStart,
		WorkspaceID:    workspaceID,
		SessionID:      "sess-fit",
		AgentID:        "agent-fit",
		TaskID:         "task-fit",
		Summary:        "agent is already attached to task-fit",
		OwnerScope:     "task/session",
		RelatedDocKeys: []string{"doc-fit-a", "doc-fit-b"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit-a"},
			{Ref: "artifact://fit-b"},
		},
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:anchors:full",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit-a", "doc-fit-b"},
		ArtifactRefs:   []string{"artifact://fit-a", "artifact://fit-b"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:anchors:partial",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-fit-a", "doc-other"},
		ArtifactRefs:   []string{"artifact://fit-a", "artifact://other"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:anchors:none",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-fit"},
		DocKeys:        []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs:   []string{"artifact://other-a", "artifact://other-b"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with graded anchor coverage: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	full := requireScoredTensionByID(t, scored, "tension:anchors:full")
	partial := requireScoredTensionByID(t, scored, "tension:anchors:partial")
	none := requireScoredTensionByID(t, scored, "tension:anchors:none")

	if full.AttachFactors.Fit <= partial.AttachFactors.Fit {
		t.Fatalf("expected full anchor coverage to outrank partial overlap, full=%+v partial=%+v", full.AttachFactors, partial.AttachFactors)
	}
	if partial.AttachFactors.Fit <= none.AttachFactors.Fit {
		t.Fatalf("expected partial anchor coverage to outrank no overlap, partial=%+v none=%+v", partial.AttachFactors, none.AttachFactors)
	}
	if scored[0].TensionID != "tension:anchors:full" || scored[1].TensionID != "tension:anchors:partial" {
		t.Fatalf("expected richer anchor coverage to drive shortlist rank, got %+v", scored)
	}
}

func TestListAgentAvailableTensionsScoredScalesInertiaPenaltiesBySessionAnchorRetention(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-session-anchor-retention"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-fit")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-fit", "node-fit")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-fit", "agent-fit")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:           model.SessionEventStart,
		WorkspaceID:         workspaceID,
		SessionID:           "sess-fit",
		AgentID:             "agent-fit",
		TaskID:              "task-fit",
		Summary:             "agent is already attached to task-fit",
		OwnerScope:          "task/session",
		RelatedDocKeys:      []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: "artifact://fit"}},
	}); err != nil {
		t.Fatalf("record active session context: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:context:full",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-switch"},
		SessionIDs:     []string{"sess-fit"},
		DocKeys:        []string{"doc-fit"},
		ArtifactRefs:   []string{"artifact://fit"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:context:partial",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-switch"},
		DocKeys:        []string{"doc-fit", "doc-other"},
		ArtifactRefs:   []string{"artifact://fit", "artifact://other"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:context:thin",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-switch"},
		DocKeys:        []string{"doc-fit", "doc-other"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:context:none",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		TaskIDs:        []string{"task-switch"},
		SessionIDs:     []string{"sess-other"},
		DocKeys:        []string{"doc-other-a", "doc-other-b"},
		ArtifactRefs:   []string{"artifact://other-a", "artifact://other-b"},
		BaseScore:      72,
		SurfaceScore:   82,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-fit")
	if err != nil {
		t.Fatalf("list scored tensions with session-anchor retention: %v", err)
	}
	if len(scored) != 4 {
		t.Fatalf("expected four scored tensions, got %+v", scored)
	}

	full := requireScoredTensionByID(t, scored, "tension:context:full")
	partial := requireScoredTensionByID(t, scored, "tension:context:partial")
	thin := requireScoredTensionByID(t, scored, "tension:context:thin")
	none := requireScoredTensionByID(t, scored, "tension:context:none")

	if full.AttachFactors.SwitchPenalty != 0 || full.AttachFactors.ContextLossPenalty != 0 {
		t.Fatalf("expected full session-anchor retention to clear inertia penalties, got %+v", full.AttachFactors)
	}
	if full.AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected full session-anchor retention to surface bounded off-task stay bonus, got %+v", full.AttachFactors)
	}
	if partial.AttachFactors.SwitchPenalty <= 0 || partial.AttachFactors.ContextLossPenalty <= 0 {
		t.Fatalf("expected partial session-anchor retention to keep bounded inertia penalties, got %+v", partial.AttachFactors)
	}
	if partial.AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected partial session-anchor retention to keep bounded off-task stay bonus, got %+v", partial.AttachFactors)
	}
	if thin.AttachFactors.SwitchPenalty <= 0 || thin.AttachFactors.ContextLossPenalty <= 0 {
		t.Fatalf("expected thin session-anchor retention to keep bounded inertia penalties, got %+v", thin.AttachFactors)
	}
	if thin.AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected thin session-anchor retention to keep bounded off-task stay bonus, got %+v", thin.AttachFactors)
	}
	if full.AttachFactors.StayBonus <= partial.AttachFactors.StayBonus {
		t.Fatalf("expected richer continuity to surface stronger stay bonus, full=%+v partial=%+v", full.AttachFactors, partial.AttachFactors)
	}
	if partial.AttachFactors.StayBonus <= thin.AttachFactors.StayBonus {
		t.Fatalf("expected partial continuity to surface stronger stay bonus than thin retention, partial=%+v thin=%+v", partial.AttachFactors, thin.AttachFactors)
	}
	if thin.AttachFactors.StayBonus <= none.AttachFactors.StayBonus {
		t.Fatalf("expected thin continuity to outrank none on stay bonus, thin=%+v none=%+v", thin.AttachFactors, none.AttachFactors)
	}
	if partial.AttachFactors.SwitchPenalty >= thin.AttachFactors.SwitchPenalty {
		t.Fatalf("expected richer session-anchor retention to reduce switch penalty more than thin overlap, partial=%+v thin=%+v", partial.AttachFactors, thin.AttachFactors)
	}
	if partial.AttachFactors.ContextLossPenalty >= thin.AttachFactors.ContextLossPenalty {
		t.Fatalf("expected richer session-anchor retention to reduce context-loss penalty more than thin overlap, partial=%+v thin=%+v", partial.AttachFactors, thin.AttachFactors)
	}
	if thin.AttachFactors.SwitchPenalty >= none.AttachFactors.SwitchPenalty {
		t.Fatalf("expected thin session-anchor retention to reduce switch penalty relative to none, thin=%+v none=%+v", thin.AttachFactors, none.AttachFactors)
	}
	if thin.AttachFactors.ContextLossPenalty >= none.AttachFactors.ContextLossPenalty {
		t.Fatalf("expected thin session-anchor retention to reduce context-loss penalty relative to none, thin=%+v none=%+v", thin.AttachFactors, none.AttachFactors)
	}
	if partial.AttachFactors.SwitchPenalty >= none.AttachFactors.SwitchPenalty {
		t.Fatalf("expected partial session-anchor retention to reduce switch penalty, partial=%+v none=%+v", partial.AttachFactors, none.AttachFactors)
	}
	if partial.AttachFactors.ContextLossPenalty >= none.AttachFactors.ContextLossPenalty {
		t.Fatalf("expected partial session-anchor retention to reduce context-loss penalty, partial=%+v none=%+v", partial.AttachFactors, none.AttachFactors)
	}
	if none.AttachFactors.SwitchPenalty <= 0 || none.AttachFactors.ContextLossPenalty <= 0 {
		t.Fatalf("expected no-retention tension to keep full inertia penalties, got %+v", none.AttachFactors)
	}
	if none.AttachFactors.StayBonus != 0 {
		t.Fatalf("expected no-retention tension to keep zero off-task stay bonus, got %+v", none.AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredSurfacesExplorationPriorForLowHistoryAgents(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-exploration-prior"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie", "agent-veteran")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:exploration",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      70,
		SurfaceScore:   85,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	for idx := 0; idx < 16; idx++ {
		if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
			EventType:   model.SessionEventStart,
			WorkspaceID: workspaceID,
			SessionID:   "sess-veteran-history-" + strconv.Itoa(idx),
			AgentID:     "agent-veteran",
			Summary:     "historical session",
			OwnerScope:  "task/session",
		}); err != nil {
			t.Fatalf("seed veteran history %d: %v", idx, err)
		}
	}

	rookie, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list rookie scored tensions: %v", err)
	}
	veteran, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-veteran")
	if err != nil {
		t.Fatalf("list veteran scored tensions: %v", err)
	}
	if len(rookie) == 0 || len(veteran) == 0 {
		t.Fatalf("expected scored tensions for both agents, rookie=%+v veteran=%+v", rookie, veteran)
	}
	if rookie[0].AttachFactors.ExplorationPrior <= veteran[0].AttachFactors.ExplorationPrior {
		t.Fatalf("expected low-history agent to surface stronger exploration prior, rookie=%+v veteran=%+v", rookie[0].AttachFactors, veteran[0].AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredKeepsExplorationPriorUnderAgentUpdateChatter(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-exploration-update-chatter"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie", "agent-chatter")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:exploration",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      70,
		SurfaceScore:   85,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	for idx := 0; idx < 16; idx++ {
		if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
			WorkspaceID: workspaceID,
			AgentID:     "agent-chatter",
			UpdateType:  "status",
			Summary:     "historical update",
			PayloadJSON: `{"index":` + strconv.Itoa(idx) + `}`,
		}); err != nil {
			t.Fatalf("seed chatter history %d: %v", idx, err)
		}
	}

	rookie, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list rookie scored tensions: %v", err)
	}
	chatter, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-chatter")
	if err != nil {
		t.Fatalf("list chatter scored tensions: %v", err)
	}
	if len(rookie) == 0 || len(chatter) == 0 {
		t.Fatalf("expected scored tensions for both agents, rookie=%+v chatter=%+v", rookie, chatter)
	}
	if chatter[0].AttachFactors.ExplorationPrior <= 0 {
		t.Fatalf("expected updates-only chatter to keep exploration prior, chatter=%+v", chatter[0].AttachFactors)
	}
	if math.Abs(rookie[0].AttachFactors.ExplorationPrior-chatter[0].AttachFactors.ExplorationPrior) > 1e-9 {
		t.Fatalf("expected updates-only chatter to keep the same exploration prior as a low-history agent, rookie=%+v chatter=%+v", rookie[0].AttachFactors, chatter[0].AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredSuppressesExplorationPriorForExplicitTargets(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-exploration-explicit-targets"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:explore:untargeted",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      35,
		SurfaceScore:   35,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:explore:agent-targeted",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-rookie"},
		BaseScore:      35,
		SurfaceScore:   35,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:explore:agent-mismatch",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-other"},
		BaseScore:      35,
		SurfaceScore:   35,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list scored tensions with explicit-target exploration candidates: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	untargeted := requireScoredTensionByID(t, scored, "tension:explore:untargeted")
	targeted := requireScoredTensionByID(t, scored, "tension:explore:agent-targeted")
	mismatch := requireScoredTensionByID(t, scored, "tension:explore:agent-mismatch")

	if untargeted.AttachFactors.ExplorationPrior <= 0 {
		t.Fatalf("expected untargeted peripheral candidate to keep exploration prior, got %+v", untargeted.AttachFactors)
	}
	if targeted.AttachFactors.ExplorationPrior != 0 {
		t.Fatalf("expected explicit targeted peripheral candidate to suppress exploration prior, got %+v", targeted.AttachFactors)
	}
	if mismatch.AttachFactors.ExplorationPrior != 0 {
		t.Fatalf("expected explicit mismatch peripheral candidate to suppress exploration prior, got %+v", mismatch.AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredInjectsPeripheralExplorationCandidate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-exploration-shortlist"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fixtures := []struct {
		id      string
		surface int
	}{
		{id: "tension:surface:95", surface: 95},
		{id: "tension:surface:90", surface: 90},
		{id: "tension:surface:85", surface: 85},
		{id: "tension:surface:35", surface: 35},
		{id: "tension:surface:25", surface: 25},
	}
	for _, fixture := range fixtures {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      fixture.id,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			BaseScore:      fixture.surface,
			SurfaceScore:   fixture.surface,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list scored tensions with exploration shortlist: %v", err)
	}
	if len(scored) != len(fixtures) {
		t.Fatalf("expected %d scored tensions, got %+v", len(fixtures), scored)
	}
	if scored[2].SurfaceScore > 35 {
		t.Fatalf("expected third shortlist slot to carry a peripheral exploration candidate, got %+v", scored[:3])
	}
	if scored[2].AttachFactors.ExplorationPrior <= 0 {
		t.Fatalf("expected exploration candidate to surface explicit exploration prior, got %+v", scored[2])
	}
}

func TestInjectExplorationCandidatePrefersRecoveryLeaseAwarePeripheralCandidate(t *testing.T) {
	t.Parallel()

	scored := []ScoredTension{
		{TensionRecord: TensionRecord{TensionID: "tension:top:95", SurfaceScore: 95}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:top:90", SurfaceScore: 90}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:mid:80", SurfaceScore: 80}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.10}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:archival", SurfaceScore: 25}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.70,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.80,
			RecoveryRisk:      0.05,
		}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:recoverable", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.66,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.60,
			LeaseSensitive:    true,
		}},
	}

	updated := injectExplorationCandidate(append([]ScoredTension(nil), scored...), 0.8)
	if updated[2].TensionID != "tension:explore:recoverable" {
		t.Fatalf("expected exploration slot to prefer recovery/lease-aware peripheral candidate over archival sibling, got %+v", updated[:3])
	}
	if updated[3].TensionID != "tension:mid:80" {
		t.Fatalf("expected exploration insertion to shift the previous third slot down by one position, got %+v", updated[:4])
	}
}

func TestInjectExplorationCandidatePrefersFarReviewerReliefPeripheralCandidate(t *testing.T) {
	t.Parallel()

	scored := []ScoredTension{
		{TensionRecord: TensionRecord{TensionID: "tension:top:95", SurfaceScore: 95}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:top:90", SurfaceScore: 90}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:mid:80", SurfaceScore: 80}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.10}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:covered", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.62,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
		}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:open-gap", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.58,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
			FarReviewerRelief: 0.90,
		}},
	}

	updated := injectExplorationCandidate(append([]ScoredTension(nil), scored...), 0.8)
	if updated[2].TensionID != "tension:explore:open-gap" {
		t.Fatalf("expected exploration slot to prefer open far-reviewer-gap peripheral candidate over covered sibling, got %+v", updated[:3])
	}
	if updated[3].TensionID != "tension:mid:80" {
		t.Fatalf("expected exploration insertion to shift the previous third slot down by one position, got %+v", updated[:4])
	}
}

func TestInjectExplorationCandidatePrefersNovelPeripheralCandidate(t *testing.T) {
	t.Parallel()

	scored := []ScoredTension{
		{TensionRecord: TensionRecord{TensionID: "tension:top:95", SurfaceScore: 95}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:top:90", SurfaceScore: 90}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:mid:80", SurfaceScore: 80}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.10}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:stale", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.62,
			Novelty:           0.20,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
		}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:fresh", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.60,
			Novelty:           0.90,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
		}},
	}

	updated := injectExplorationCandidate(append([]ScoredTension(nil), scored...), 0.8)
	if updated[2].TensionID != "tension:explore:fresh" {
		t.Fatalf("expected exploration slot to prefer fresher peripheral candidate over staler sibling, got %+v", updated[:3])
	}
	if updated[3].TensionID != "tension:mid:80" {
		t.Fatalf("expected exploration insertion to shift the previous third slot down by one position, got %+v", updated[:4])
	}
}

func TestInjectExplorationCandidatePrefersViablePeripheralCandidate(t *testing.T) {
	t.Parallel()

	scored := []ScoredTension{
		{TensionRecord: TensionRecord{TensionID: "tension:top:95", SurfaceScore: 95}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:top:90", SurfaceScore: 90}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:mid:80", SurfaceScore: 80}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.10}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:barely-attachable", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.62,
			Fit:               0.30,
			Novelty:           0.80,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
		}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:viable", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:  0.60,
			Fit:               0.90,
			Novelty:           0.80,
			CrowdingRatio:     0.10,
			ArchivePropensity: 0.10,
			RecoveryRisk:      0.10,
		}},
	}

	updated := injectExplorationCandidate(append([]ScoredTension(nil), scored...), 0.8)
	if updated[2].TensionID != "tension:explore:viable" {
		t.Fatalf("expected exploration slot to prefer more attachable peripheral candidate over barely attachable sibling, got %+v", updated[:3])
	}
	if updated[3].TensionID != "tension:mid:80" {
		t.Fatalf("expected exploration insertion to shift the previous third slot down by one position, got %+v", updated[:4])
	}
}

func TestInjectExplorationCandidatePrefersLowerInertiaPeripheralCandidate(t *testing.T) {
	t.Parallel()

	scored := []ScoredTension{
		{TensionRecord: TensionRecord{TensionID: "tension:top:95", SurfaceScore: 95}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:top:90", SurfaceScore: 90}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.05}},
		{TensionRecord: TensionRecord{TensionID: "tension:mid:80", SurfaceScore: 80}, AttachFactors: AgentAttachmentFactors{ExplorationPrior: 0.10}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:high-inertia", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:   0.62,
			Fit:                0.85,
			Novelty:            0.80,
			CrowdingRatio:      0.10,
			ArchivePropensity:  0.10,
			RecoveryRisk:       0.10,
			StayBonus:          0.00,
			SwitchPenalty:      0.60,
			ContextLossPenalty: 0.60,
		}},
		{TensionRecord: TensionRecord{TensionID: "tension:explore:retained", SurfaceScore: 35}, AttachFactors: AgentAttachmentFactors{
			ExplorationPrior:   0.60,
			Fit:                0.85,
			Novelty:            0.80,
			CrowdingRatio:      0.10,
			ArchivePropensity:  0.10,
			RecoveryRisk:       0.10,
			StayBonus:          0.30,
			SwitchPenalty:      0.10,
			ContextLossPenalty: 0.10,
		}},
	}

	updated := injectExplorationCandidate(append([]ScoredTension(nil), scored...), 0.8)
	if updated[2].TensionID != "tension:explore:retained" {
		t.Fatalf("expected exploration slot to prefer lower-inertia peripheral candidate over high-inertia sibling, got %+v", updated[:3])
	}
	if updated[3].TensionID != "tension:mid:80" {
		t.Fatalf("expected exploration insertion to shift the previous third slot down by one position, got %+v", updated[:4])
	}
}

func TestListAgentAvailableTensionsScoredKeepsExplorationCandidateUnderCurrentTaskInertiaAndCrowding(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-inertia-exploration"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie", "agent-busy-a", "agent-busy-b", "agent-busy-c")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-current", "node-current")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-explore-a", "node-explore-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-explore-b", "node-explore-b")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-current", "agent-rookie")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-rookie-current",
		AgentID:     "agent-rookie",
		TaskID:      "task-current",
		Summary:     "rookie is already attached to task-current",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record rookie current-task session: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, fixture := range []struct {
		id      string
		taskID  string
		surface int
	}{
		{id: "tension:current:95", taskID: "task-current", surface: 95},
		{id: "tension:current:90", taskID: "task-current", surface: 90},
		{id: "tension:current:85", taskID: "task-current", surface: 85},
		{id: "tension:explore:uncrowded", taskID: "task-explore-a", surface: 35},
		{id: "tension:explore:crowded", taskID: "task-explore-b", surface: 25},
	} {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      fixture.id,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{fixture.taskID},
			BaseScore:      fixture.surface,
			SurfaceScore:   fixture.surface,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	coalition, err := store.CreateCoalition(ctx, workspaceID, "tension:explore:crowded", "busy peripheral tension")
	if err != nil {
		t.Fatalf("create crowded exploration coalition: %v", err)
	}
	for _, agentID := range []string{"agent-busy-a", "agent-busy-b"} {
		if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agentID, 0.7, 0.5); err != nil {
			t.Fatalf("add crowded exploration member %s: %v", agentID, err)
		}
	}

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list scored tensions under current-task inertia: %v", err)
	}
	if len(scored) != 5 {
		t.Fatalf("expected five scored tensions, got %+v", scored)
	}
	if !containsString(scored[0].TaskIDs, "task-current") || !containsString(scored[1].TaskIDs, "task-current") {
		t.Fatalf("expected current-task tensions to keep the top two slots, got %+v", scored[:2])
	}
	if scored[0].AttachFactors.StayBonus <= 0 || scored[1].AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected top current-task tensions to retain stay-bonus inertia, got %+v %+v", scored[0].AttachFactors, scored[1].AttachFactors)
	}
	if scored[2].TensionID != "tension:explore:uncrowded" {
		t.Fatalf("expected shortlist slot three to keep an uncrowded exploration candidate, got %+v", scored[:3])
	}
	if containsString(scored[2].TaskIDs, "task-current") {
		t.Fatalf("expected exploration slot to break current-task inertia, got %+v", scored[2])
	}
	if scored[2].AttachFactors.ExplorationPrior <= 0 {
		t.Fatalf("expected exploration slot to expose explicit exploration prior, got %+v", scored[2].AttachFactors)
	}
	if scored[2].AttachFactors.SwitchPenalty <= 0 || scored[2].AttachFactors.ContextLossPenalty <= 0 {
		t.Fatalf("expected exploration slot to remain off-task and pay switch/context penalties, got %+v", scored[2].AttachFactors)
	}

	crowdedIdx := -1
	for idx := range scored {
		if scored[idx].TensionID == "tension:explore:crowded" {
			crowdedIdx = idx
			break
		}
	}
	if crowdedIdx == -1 {
		t.Fatalf("expected crowded exploration tension to remain in shortlist, got %+v", scored)
	}
	if crowdedIdx <= 2 {
		t.Fatalf("expected crowded exploration tension to rank behind the uncrowded exploration slot, got %+v", scored)
	}
	if scored[crowdedIdx].AttachFactors.CrowdingRatio <= scored[2].AttachFactors.CrowdingRatio {
		t.Fatalf("expected crowded exploration option to carry a stronger crowding penalty, got chosen=%+v crowded=%+v", scored[2].AttachFactors, scored[crowdedIdx].AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredPrefersColderPeripheralExplorationCandidateOverRetainedContinuity(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-exploration-retention"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-rookie")
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-current", "node-current")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-retained", "node-retained")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, "task-cold", "node-cold")

	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, "task-current", "agent-rookie")
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:      model.SessionEventStart,
		WorkspaceID:    workspaceID,
		SessionID:      "sess-rookie-current",
		AgentID:        "agent-rookie",
		TaskID:         "task-current",
		Summary:        "rookie is already attached to task-current",
		OwnerScope:     "task/session",
		RelatedDocKeys: []string{"doc-fit"},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: "artifact://fit"},
		},
	}); err != nil {
		t.Fatalf("record rookie retained continuity session: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, fixture := range []tensionRecordFixture{
		{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:current:95",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{"task-current"},
			BaseScore:      95,
			SurfaceScore:   95,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:current:90",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{"task-current"},
			BaseScore:      90,
			SurfaceScore:   90,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:current:85",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{"task-current"},
			BaseScore:      85,
			SurfaceScore:   85,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:explore:retained",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{"task-retained"},
			DocKeys:        []string{"doc-fit"},
			ArtifactRefs:   []string{"artifact://fit"},
			BaseScore:      35,
			SurfaceScore:   35,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			WorkspaceID:    workspaceID,
			TensionID:      "tension:explore:cold",
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			TaskIDs:        []string{"task-cold"},
			BaseScore:      35,
			SurfaceScore:   35,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	} {
		insertTensionRecordFixture(t, ctx, store, fixture)
	}

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-rookie")
	if err != nil {
		t.Fatalf("list scored tensions with retained continuity exploration candidates: %v", err)
	}
	if len(scored) != 5 {
		t.Fatalf("expected five scored tensions, got %+v", scored)
	}
	if !containsString(scored[0].TaskIDs, "task-current") || !containsString(scored[1].TaskIDs, "task-current") {
		t.Fatalf("expected top two shortlist slots to remain on current-task tensions, got %+v", scored[:2])
	}
	if scored[2].TensionID != "tension:explore:cold" {
		t.Fatalf("expected colder peripheral candidate to win exploration slot over retained continuity candidate, got %+v", scored[:3])
	}

	retainedIdx := -1
	for idx := range scored {
		if scored[idx].TensionID == "tension:explore:retained" {
			retainedIdx = idx
			break
		}
	}
	if retainedIdx == -1 {
		t.Fatalf("expected retained continuity candidate to remain in shortlist, got %+v", scored)
	}
	if scored[2].AttachFactors.ExplorationPrior <= scored[retainedIdx].AttachFactors.ExplorationPrior {
		t.Fatalf("expected cold peripheral candidate to keep stronger exploration prior than retained continuity candidate, cold=%+v retained=%+v", scored[2].AttachFactors, scored[retainedIdx].AttachFactors)
	}
	if scored[retainedIdx].AttachFactors.ExplorationPrior <= 0 {
		t.Fatalf("expected retained continuity candidate to keep bounded exploration prior instead of being zeroed, got %+v", scored[retainedIdx].AttachFactors)
	}
	if scored[retainedIdx].AttachFactors.StayBonus <= 0 {
		t.Fatalf("expected retained continuity candidate to keep bounded off-task stay bonus, got %+v", scored[retainedIdx].AttachFactors)
	}
	if scored[retainedIdx].AttachFactors.SwitchPenalty >= scored[2].AttachFactors.SwitchPenalty || scored[retainedIdx].AttachFactors.ContextLossPenalty >= scored[2].AttachFactors.ContextLossPenalty {
		t.Fatalf("expected retained continuity candidate to keep bounded inertia relief relative to cold candidate, retained=%+v cold=%+v", scored[retainedIdx].AttachFactors, scored[2].AttachFactors)
	}
}

func TestListAgentAvailableTensionsScoredPrefersCoalitionWithMoreDistantGeneratorWhenOccupancyMatches(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-generator-distance"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-probe", "agent-generator-near", "agent-generator-far", "agent-reviewer-a", "agent-reviewer-b")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, fixture := range []string{"tension:distance:near", "tension:distance:far"} {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      fixture,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			BaseScore:      82,
			SurfaceScore:   82,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-only", "probe-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-near", "near-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-near", "near-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-near", "near-only", "near-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-far", "far-only-b", "far-only-b")

	nearCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:distance:near", "near generator coalition")
	if err != nil {
		t.Fatalf("create near coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, nearCoalition.CoalitionID, "agent-generator-near", 0.9, 0.2); err != nil {
		t.Fatalf("add near generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, nearCoalition.CoalitionID, "agent-reviewer-a", 0.7, 0.4); err != nil {
		t.Fatalf("add near reviewer: %v", err)
	}

	farCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:distance:far", "far generator coalition")
	if err != nil {
		t.Fatalf("create far coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, farCoalition.CoalitionID, "agent-generator-far", 0.9, 0.2); err != nil {
		t.Fatalf("add far generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, farCoalition.CoalitionID, "agent-reviewer-b", 0.7, 0.4); err != nil {
		t.Fatalf("add far reviewer: %v", err)
	}

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-probe")
	if err != nil {
		t.Fatalf("list scored tensions for probe agent: %v", err)
	}

	nearItem := requireScoredTensionByID(t, scored, "tension:distance:near")
	farItem := requireScoredTensionByID(t, scored, "tension:distance:far")

	if nearItem.AttachFactors.CrowdingRatio != farItem.AttachFactors.CrowdingRatio {
		t.Fatalf("expected matched coalition occupancy for distance comparison, near=%+v far=%+v", nearItem.AttachFactors, farItem.AttachFactors)
	}
	if farItem.AttachFactors.Novelty <= nearItem.AttachFactors.Novelty {
		t.Fatalf("expected distant-generator coalition to surface stronger novelty in attach factors, near=%+v far=%+v", nearItem.AttachFactors, farItem.AttachFactors)
	}
	if farItem.AttachScore <= nearItem.AttachScore {
		t.Fatalf("expected distant-generator coalition to outrank near coalition once generator-distance bias is folded into novelty, near=%+v far=%+v", nearItem, farItem)
	}
	if farItem.AttachProb <= nearItem.AttachProb {
		t.Fatalf("expected distant-generator coalition to carry higher attach probability once distance bias is active, near=%+v far=%+v", nearItem, farItem)
	}
}

func TestListAgentAvailableTensionsScoredCouplesFrontierPressureIntoShortlist(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-frontier-pressure"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-probe", "agent-busy")
	ensureTensionOverlayTables(t, ctx, store)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-probe",
		AgentID:     "agent-probe",
		Summary:     "probe session keeps current continuity available for matched-fit pressure checks",
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record probe session context: %v", err)
	}

	now := time.Now().UTC()
	recent := now.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	stale := now.Add(-10 * 24 * time.Hour).Format(time.RFC3339Nano)

	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:pressure:balanced",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      82,
		SurfaceScore:   82,
		EvidenceCount:  2,
		LastSeenAt:     recent,
		CreatedAt:      recent,
		UpdatedAt:      recent,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:pressure:archival",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		BaseScore:      82,
		SurfaceScore:   82,
		EvidenceCount:  1,
		LastSeenAt:     stale,
		CreatedAt:      stale,
		UpdatedAt:      stale,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:pressure:leased",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		AgentIDs:       []string{"agent-probe"},
		SessionIDs:     []string{"sess-probe"},
		SegmentRefs:    []string{"workspace_doc:" + workspaceID + "/runbook#root"},
		ConstraintRefs: []string{"constraint://coordination-lock"},
		BaseScore:      82,
		SurfaceScore:   82,
		EvidenceCount:  2,
		LastSeenAt:     recent,
		CreatedAt:      recent,
		UpdatedAt:      recent,
	})

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-probe")
	if err != nil {
		t.Fatalf("list scored tensions with frontier pressure: %v", err)
	}
	if len(scored) != 3 {
		t.Fatalf("expected three scored tensions, got %+v", scored)
	}

	balanced := requireScoredTensionByID(t, scored, "tension:pressure:balanced")
	archival := requireScoredTensionByID(t, scored, "tension:pressure:archival")
	leased := requireScoredTensionByID(t, scored, "tension:pressure:leased")

	if leased.AttachScore <= balanced.AttachScore {
		t.Fatalf("expected lease/recovery pressure to lift shortlist rank under matched fit, balanced=%+v leased=%+v", balanced, leased)
	}
	if balanced.AttachScore <= archival.AttachScore {
		t.Fatalf("expected archival pressure to reduce shortlist rank under matched fit, balanced=%+v archival=%+v", balanced, archival)
	}
	if !leased.AttachFactors.LeaseSensitive || leased.AttachFactors.RecoveryRisk <= balanced.AttachFactors.RecoveryRisk {
		t.Fatalf("expected leased candidate to surface stronger recovery/lease factors, balanced=%+v leased=%+v", balanced.AttachFactors, leased.AttachFactors)
	}
	if archival.AttachFactors.ArchivePropensity <= balanced.AttachFactors.ArchivePropensity {
		t.Fatalf("expected archival candidate to surface stronger archive propensity, balanced=%+v archival=%+v", balanced.AttachFactors, archival.AttachFactors)
	}
	if balanced.AttachDecision.State != AttachmentDecisionAllowed {
		t.Fatalf("expected balanced frontier candidate to remain attach-allowed, got %+v", balanced.AttachDecision)
	}
	if leased.AttachDecision.State != AttachmentDecisionGuarded {
		t.Fatalf("expected lease-sensitive frontier candidate to become attach-guarded, got %+v", leased.AttachDecision)
	}
	if !containsString(leased.AttachDecision.Reasons, "lease_sensitive_coordination") {
		t.Fatalf("expected guarded lease-sensitive frontier candidate to cite coordination reason, got %+v", leased.AttachDecision)
	}
}

func TestListAgentAvailableTensionsScoredPrefersCoalitionWhereCandidateCanRelieveFarReviewerGap(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-attach-far-reviewer-relief"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-probe", "agent-generator-gap", "agent-generator-covered", "agent-reviewer-near", "agent-reviewer-far")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, fixture := range []string{"tension:far-gap:open", "tension:far-gap:covered"} {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      fixture,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			ReviewStatus:   tensionReviewPending,
			BaseScore:      82,
			SurfaceScore:   82,
			EvidenceCount:  2,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-shared-gap", "shared-gap")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-shared-covered", "shared-covered")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-probe", "probe-only", "probe-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-gap", "gap-shared", "shared-gap")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-gap", "gap-near-shared-b", "gap-near-shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-gap", "gap-near-shared-c", "gap-near-shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-gap", "gap-only-a", "gap-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-gap", "gap-only-b", "gap-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-covered", "covered-shared", "shared-covered")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-covered", "covered-only-a", "covered-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-covered", "covered-only-b", "covered-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-near", "near-shared-gap", "shared-gap")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-near", "near-gap-shared-b", "gap-near-shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-near", "near-gap-shared-c", "gap-near-shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-near", "near-gap-only-a", "gap-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-near", "near-only", "near-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-far", "far-shared-covered", "shared-covered")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-reviewer-far", "far-only-b", "far-only-b")

	openCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:far-gap:open", "open far reviewer gap coalition")
	if err != nil {
		t.Fatalf("create open gap coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, openCoalition.CoalitionID, "agent-generator-gap", 0.9, 0.2); err != nil {
		t.Fatalf("add open-gap generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, openCoalition.CoalitionID, "agent-reviewer-near", 0.7, 0.4); err != nil {
		t.Fatalf("add open-gap near reviewer: %v", err)
	}

	coveredCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:far-gap:covered", "covered far reviewer coalition")
	if err != nil {
		t.Fatalf("create covered coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coveredCoalition.CoalitionID, "agent-generator-covered", 0.9, 0.2); err != nil {
		t.Fatalf("add covered generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coveredCoalition.CoalitionID, "agent-reviewer-far", 0.7, 0.4); err != nil {
		t.Fatalf("add covered far reviewer: %v", err)
	}
	forceRole := func(coalitionID, agentID, role string) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx,
			`UPDATE workspace_coalition_members SET role = ? WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			role, workspaceID, coalitionID, agentID,
		); err != nil {
			t.Fatalf("force coalition role %s/%s -> %s: %v", coalitionID, agentID, role, err)
		}
	}
	forceRole(openCoalition.CoalitionID, "agent-generator-gap", "GENERATOR")
	forceRole(openCoalition.CoalitionID, "agent-reviewer-near", "NEAR_REVIEWER")
	forceRole(coveredCoalition.CoalitionID, "agent-generator-covered", "GENERATOR")
	forceRole(coveredCoalition.CoalitionID, "agent-reviewer-far", "FAR_REVIEWER")

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-probe")
	if err != nil {
		t.Fatalf("list scored tensions for far-reviewer relief: %v", err)
	}

	openItem := requireScoredTensionByID(t, scored, "tension:far-gap:open")
	coveredItem := requireScoredTensionByID(t, scored, "tension:far-gap:covered")

	if openItem.AttachFactors.CrowdingRatio != coveredItem.AttachFactors.CrowdingRatio {
		t.Fatalf("expected matched coalition occupancy for far-reviewer relief comparison, open=%+v covered=%+v", openItem.AttachFactors, coveredItem.AttachFactors)
	}
	if openItem.AttachFactors.FarReviewerRelief <= 0 {
		t.Fatalf("expected open far-reviewer gap to surface bounded relief, open=%+v", openItem.AttachFactors)
	}
	if coveredItem.AttachFactors.FarReviewerRelief != 0 {
		t.Fatalf("expected covered coalition to suppress far-reviewer relief, covered=%+v", coveredItem.AttachFactors)
	}
	if openItem.AttachScore <= coveredItem.AttachScore {
		t.Fatalf("expected far-reviewer relief to lift shortlist rank for the open-gap coalition, open=%+v covered=%+v", openItem, coveredItem)
	}
	if openItem.AttachProb <= coveredItem.AttachProb {
		t.Fatalf("expected far-reviewer relief to lift shortlist probability for the open-gap coalition, open=%+v covered=%+v", openItem, coveredItem)
	}
}

func joinScoredTensionIDs(items []ScoredTension) string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.TensionID)
	}
	return strings.Join(ids, ",")
}

func requireScoredTensionByID(t *testing.T, items []ScoredTension, tensionID string) ScoredTension {
	t.Helper()

	for _, item := range items {
		if item.TensionID == tensionID {
			return item
		}
	}
	t.Fatalf("expected scored tension %s in %+v", tensionID, items)
	return ScoredTension{}
}
