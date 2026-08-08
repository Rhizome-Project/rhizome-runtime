package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestGovernanceChallengeReassignsProjectLeadAfterStrictQuorum(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-governance-reassign"
		projectID   = "project-governance"
		leadID      = "alpha"
		challenger  = "beta"
		successor   = "gamma"
		voter       = "delta"
	)
	seedGovernanceWorkspace(t, ctx, store, workspaceID, projectID, leadID, challenger, successor, voter)

	results, err := store.ValidateGovernanceStallPredicates(ctx, workspaceID, projectID, leadID, nil)
	if err != nil {
		t.Fatalf("validate predicates: %v", err)
	}
	if len(results) != 2 || !results[0].Holds || !results[1].Holds {
		t.Fatalf("expected strict governance predicates to hold, got %+v", results)
	}

	challenge, raisedEvent, err := store.RaiseGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeRaiseInput{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ChallengedAgentID:         leadID,
		ChallengerAgentID:         challenger,
		NominatedSuccessorAgentID: successor,
		ActorID:                   challenger,
		ActorType:                 "agent",
		ArgumentDocKey:            "docs/governance/challenge",
		PromptContextEnvelope:     sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.raise", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:      "project.governance.challenge.raise",
	})
	if err != nil {
		t.Fatalf("raise governance challenge: %v", err)
	}
	if challenge.State != sqlite.GovernanceChallengeStateDefenseOpen || raisedEvent.EventType != "governance.challenge.raised" {
		t.Fatalf("unexpected challenge/event after raise: challenge=%+v event=%+v", challenge, raisedEvent)
	}

	challenge, defendedEvent, err := store.DefendGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeDefendInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               leadID,
		ActorType:             "agent",
		Stance:                "DEFEND",
		DefenseDocKey:         "docs/governance/defense",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.defend", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.governance.challenge.defend",
	})
	if err != nil {
		t.Fatalf("defend governance challenge: %v", err)
	}
	if challenge.State != sqlite.GovernanceChallengeStateVoting || defendedEvent.EventType != "governance.challenge.defended" {
		t.Fatalf("unexpected challenge/event after defense: challenge=%+v event=%+v", challenge, defendedEvent)
	}
	assertGovernanceVoteAdvisories(t, ctx, store, workspaceID, challenge.ChallengeID, []string{leadID, challenger, successor, voter})

	for _, agentID := range []string{challenger, successor, voter} {
		if _, _, err := store.VoteGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeVoteInput{
			WorkspaceID:           workspaceID,
			ChallengeID:           challenge.ChallengeID,
			VoterAgentID:          agentID,
			Ballot:                sqlite.GovernanceBallotReassign,
			ActorID:               agentID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.vote.cast", "server_rpc", workspaceID, "agent", agentID),
			PromptContextSurface:  "project.governance.vote.cast",
		}); err != nil {
			t.Fatalf("cast governance vote for %s: %v", agentID, err)
		}
	}

	tally, err := store.TallyGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeTallyInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               challenger,
		ActorType:             "agent",
		ReassignEnabled:       true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.tally", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:  "project.governance.challenge.tally",
	})
	if err != nil {
		t.Fatalf("tally governance challenge: %v", err)
	}
	if tally.Challenge.State != sqlite.GovernanceChallengeStateResolvedReassigned || tally.ReassignVotes != 3 || tally.QuorumThreshold != 3 {
		t.Fatalf("unexpected tally result: %+v", tally)
	}
	if tally.LeadRole == nil || tally.LeadRole.AgentID != successor || tally.LeadTransferEvent == nil || tally.LeadTransferEvent.EventType != "project.lead.transferred" {
		t.Fatalf("expected governance tally to transfer project lead, got %+v", tally)
	}
	activeLead, ok, err := store.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get active lead: %v", err)
	}
	if !ok || activeLead.AgentID != successor {
		t.Fatalf("active strategic lead = %+v ok=%v, want %s", activeLead, ok, successor)
	}
}

func TestGovernanceDeadlineSweepForceClosesInsufficientQuorum(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-governance-deadline"
		projectID   = "project-governance-deadline"
		leadID      = "alpha"
		challenger  = "beta"
		successor   = "gamma"
		voter       = "delta"
	)
	seedGovernanceWorkspace(t, ctx, store, workspaceID, projectID, leadID, challenger, successor, voter)
	challenge, _, err := store.RaiseGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeRaiseInput{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ChallengedAgentID:         leadID,
		ChallengerAgentID:         challenger,
		NominatedSuccessorAgentID: successor,
		ActorID:                   challenger,
		ActorType:                 "agent",
		PromptContextEnvelope:     sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.raise", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:      "project.governance.challenge.raise",
	})
	if err != nil {
		t.Fatalf("raise governance challenge: %v", err)
	}
	challenge, _, err = store.DefendGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeDefendInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               leadID,
		ActorType:             "agent",
		Stance:                "DEFEND",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.defend", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.governance.challenge.defend",
	})
	if err != nil {
		t.Fatalf("defend governance challenge: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE governance_challenges
   SET voting_deadline_at = '2000-01-01T00:00:00Z'
 WHERE workspace_id = ? AND challenge_id = ?`, workspaceID, challenge.ChallengeID); err != nil {
		t.Fatalf("force expired governance deadline: %v", err)
	}
	result, err := store.SweepExpiredGovernanceChallengesWithEvent(ctx, 10)
	if err != nil {
		t.Fatalf("sweep expired governance challenges: %v", err)
	}
	if result.Scanned != 1 || result.Resolved != 1 || result.Failed != 0 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	resolved, err := store.GetGovernanceChallenge(ctx, workspaceID, challenge.ChallengeID)
	if err != nil {
		t.Fatalf("get resolved challenge: %v", err)
	}
	if resolved.State != sqlite.GovernanceChallengeStateResolvedDefault || !strings.Contains(resolved.Resolution, "insufficient quorum") {
		t.Fatalf("expected default insufficient-quorum resolution, got %+v", resolved)
	}
}

func TestGovernanceChallengeRejectsWhenFanoutAlreadyExists(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-governance-predicate-reject"
		projectID   = "project-governance-reject"
		leadID      = "alpha"
		challenger  = "beta"
		successor   = "gamma"
	)
	seedGovernanceWorkspace(t, ctx, store, workspaceID, projectID, leadID, challenger, successor, "delta")
	createGovernanceImplementationTask(t, ctx, store, workspaceID, projectID, "task-existing-impl")

	results, err := store.ValidateGovernanceStallPredicates(ctx, workspaceID, projectID, leadID, nil)
	if err != nil {
		t.Fatalf("validate predicates: %v", err)
	}
	if results[0].Name != sqlite.GovernancePredicateFanoutAbsent || results[0].Holds {
		t.Fatalf("expected fanout_absent predicate to fail with an open implementation task, got %+v", results)
	}
	if _, _, err := store.RaiseGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeRaiseInput{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ChallengedAgentID:         leadID,
		ChallengerAgentID:         challenger,
		NominatedSuccessorAgentID: successor,
		ActorID:                   challenger,
		ActorType:                 "agent",
		PromptContextEnvelope:     sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.raise", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:      "project.governance.challenge.raise",
	}); !errors.Is(err, sqlite.ErrGovernancePredicateRejected) {
		t.Fatalf("raise governance challenge err = %v, want ErrGovernancePredicateRejected", err)
	}
}

func TestGovernanceChallengeConcedeStillOpensVoteAndRequiresExplicitReassign(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-governance-concede"
		projectID   = "project-governance-concede"
		leadID      = "alpha"
		challenger  = "beta"
		successor   = "gamma"
		voter       = "delta"
	)
	seedGovernanceWorkspace(t, ctx, store, workspaceID, projectID, leadID, challenger, successor, voter)

	challenge, _, err := store.RaiseGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeRaiseInput{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ChallengedAgentID:         leadID,
		ChallengerAgentID:         challenger,
		NominatedSuccessorAgentID: successor,
		ActorID:                   challenger,
		ActorType:                 "agent",
		PromptContextEnvelope:     sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.raise", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:      "project.governance.challenge.raise",
	})
	if err != nil {
		t.Fatalf("raise governance challenge: %v", err)
	}
	challenge, _, err = store.DefendGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeDefendInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               leadID,
		ActorType:             "agent",
		Stance:                "CONCEDE",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.defend", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.governance.challenge.defend",
	})
	if err != nil {
		t.Fatalf("concede governance challenge: %v", err)
	}
	if challenge.State != sqlite.GovernanceChallengeStateVoting || challenge.DefenseStance != "CONCEDE" {
		t.Fatalf("expected conceded challenge to enter voting, got %+v", challenge)
	}
	for _, agentID := range []string{challenger, successor, voter} {
		if _, _, err := store.VoteGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeVoteInput{
			WorkspaceID:           workspaceID,
			ChallengeID:           challenge.ChallengeID,
			VoterAgentID:          agentID,
			Ballot:                sqlite.GovernanceBallotReassign,
			ActorID:               agentID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.vote.cast", "server_rpc", workspaceID, "agent", agentID),
			PromptContextSurface:  "project.governance.vote.cast",
		}); err != nil {
			t.Fatalf("cast governance vote for %s: %v", agentID, err)
		}
	}
	if _, err := store.TallyGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeTallyInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               challenger,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.tally", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:  "project.governance.challenge.tally",
	}); !errors.Is(err, sqlite.ErrGovernanceReassignmentDisabled) {
		t.Fatalf("tally without explicit reassign err = %v, want ErrGovernanceReassignmentDisabled", err)
	}
	tally, err := store.TallyGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeTallyInput{
		WorkspaceID:           workspaceID,
		ChallengeID:           challenge.ChallengeID,
		ActorID:               challenger,
		ActorType:             "agent",
		ReassignEnabled:       true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.governance.challenge.tally", "server_rpc", workspaceID, "agent", challenger),
		PromptContextSurface:  "project.governance.challenge.tally",
	})
	if err != nil {
		t.Fatalf("tally after explicit reassign: %v", err)
	}
	if tally.Challenge.State != sqlite.GovernanceChallengeStateResolvedReassigned || tally.LeadRole == nil || tally.LeadRole.AgentID != successor {
		t.Fatalf("expected conceded challenge to reassign after explicit tally, got %+v", tally)
	}
}

func seedGovernanceWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, leadID string, agentIDs ...string) {
	t.Helper()
	allAgents := append([]string{leadID}, agentIDs...)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, allAgents)
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Governance Project",
		Description: "Project used to exercise quorum leadership challenge contracts.",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               leadID,
		ActorType:             "agent",
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "open implementation fanout",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
}

func createGovernanceImplementationTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID string) {
	t.Helper()
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Existing implementation lane",
		Description:         "Existing product implementation task.",
		TaskKind:            model.TaskKindExecution,
		ProjectID:           projectID,
		ProjectLane:         "implementation",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create implementation task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach implementation task: %v", err)
	}
}

func assertGovernanceVoteAdvisories(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, challengeID string, agentIDs []string) {
	t.Helper()
	for _, agentID := range agentIDs {
		raw, err := store.GetAgentState(ctx, workspaceID, agentID, "rnar.runtime.v1")
		if err != nil {
			t.Fatalf("get scratch state for %s: %v", agentID, err)
		}
		var state struct {
			AdvisorySignals []string `json:"advisory_signals"`
		}
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			t.Fatalf("decode scratch state for %s: %v", agentID, err)
		}
		joined := strings.Join(state.AdvisorySignals, "\n")
		if !strings.Contains(joined, "GOVERNANCE VOTE OPEN: challenge "+challengeID) ||
			!strings.Contains(joined, "project_governance_challenge action=list") ||
			!strings.Contains(joined, "action=vote") {
			t.Fatalf("missing governance vote advisory for %s: %+v", agentID, state.AdvisorySignals)
		}
	}
}
