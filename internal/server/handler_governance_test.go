package server

import (
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectGovernanceRPCChallengeTallyTransfersLead(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-governance-rpc"
		projectID   = "project-governance-rpc"
		operatorID  = "operator"
		leadID      = "alpha"
		challenger  = "beta"
		successor   = "gamma"
		voter       = "delta"
	)
	seedProjectRolesWorkspace(t, testAuthContext(workspaceID, "human", operatorID), store, workspaceID, operatorID, leadID, challenger, successor, voter)
	if _, rpcErr := h.projectCreate(testAuthContext(workspaceID, "human", operatorID), mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Governance RPC",
		CreatedBy:   operatorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	leadResult, rpcErr := h.projectLeadClaim(testAuthContext(workspaceID, "agent", leadID), mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      leadID,
		AgentID:      leadID,
		LeaseSeconds: 900,
	}))
	if rpcErr != nil {
		t.Fatalf("project.lead.claim rpc error: %+v", rpcErr)
	}
	leadRole := leadResult.(map[string]any)["role"].(sqlite.ProjectRoleRecord)
	if _, rpcErr := h.projectPhaseTransition(testAuthContext(workspaceID, "agent", leadID), mustJSONRaw(projectPhaseTransitionParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     leadID,
		ToPhase:     sqlite.ProjectPhaseImplementation,
		Reason:      "open implementation fanout",
	})); rpcErr != nil {
		t.Fatalf("project.phase.transition rpc error: %+v", rpcErr)
	}

	raised, rpcErr := h.projectGovernanceChallengeRaise(testAuthContext(workspaceID, "agent", challenger), mustJSONRaw(projectGovernanceChallengeRaiseParams{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ActorID:                   challenger,
		ChallengedAgentID:         leadID,
		ChallengerAgentID:         challenger,
		NominatedSuccessorAgentID: successor,
	}))
	if rpcErr != nil {
		t.Fatalf("project.governance.challenge.raise rpc error: %+v", rpcErr)
	}
	challenge := raised.(map[string]any)["challenge"].(sqlite.GovernanceChallengeRecord)
	if challenge.State != sqlite.GovernanceChallengeStateDefenseOpen || challenge.LeadRoleID != leadRole.RoleID {
		t.Fatalf("unexpected raised challenge: %+v", challenge)
	}
	if _, rpcErr := h.projectGovernanceChallengeDefend(testAuthContext(workspaceID, "agent", leadID), mustJSONRaw(projectGovernanceChallengeDefendParams{
		WorkspaceID: workspaceID,
		ActorID:     leadID,
		ChallengeID: challenge.ChallengeID,
		Stance:      "DEFEND",
	})); rpcErr != nil {
		t.Fatalf("project.governance.challenge.defend rpc error: %+v", rpcErr)
	}
	for _, agentID := range []string{challenger, successor, voter} {
		if _, rpcErr := h.projectGovernanceVoteCast(testAuthContext(workspaceID, "agent", agentID), mustJSONRaw(projectGovernanceVoteCastParams{
			WorkspaceID:  workspaceID,
			ActorID:      agentID,
			ChallengeID:  challenge.ChallengeID,
			VoterAgentID: agentID,
			Ballot:       sqlite.GovernanceBallotReassign,
		})); rpcErr != nil {
			t.Fatalf("project.governance.vote.cast %s rpc error: %+v", agentID, rpcErr)
		}
	}
	tallyResult, rpcErr := h.projectGovernanceChallengeTally(testAuthContext(workspaceID, "agent", challenger), mustJSONRaw(projectGovernanceChallengeTallyParams{
		WorkspaceID:     workspaceID,
		ActorID:         challenger,
		ChallengeID:     challenge.ChallengeID,
		ReassignEnabled: true,
	}))
	if rpcErr != nil {
		t.Fatalf("project.governance.challenge.tally rpc error: %+v", rpcErr)
	}
	tally := tallyResult.(map[string]any)["tally"].(sqlite.GovernanceChallengeTallyResult)
	if tally.Challenge.State != sqlite.GovernanceChallengeStateResolvedReassigned || tally.LeadRole == nil || tally.LeadRole.AgentID != successor {
		t.Fatalf("unexpected tally: %+v", tally)
	}
}

func TestProjectGovernanceRPCAgentCannotRaiseForDifferentChallenger(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-governance-rpc-auth"
		projectID   = "project-governance-rpc-auth"
		operatorID  = "operator"
		leadID      = "alpha"
		challenger  = "beta"
		impostor    = "epsilon"
	)
	seedProjectRolesWorkspace(t, testAuthContext(workspaceID, "human", operatorID), store, workspaceID, operatorID, leadID, challenger, impostor)
	if _, rpcErr := h.projectCreate(testAuthContext(workspaceID, "human", operatorID), mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Governance RPC Auth",
		CreatedBy:   operatorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectGovernanceChallengeRaise(testAuthContext(workspaceID, "agent", impostor), mustJSONRaw(projectGovernanceChallengeRaiseParams{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		ActorID:           impostor,
		ChallengedAgentID: leadID,
		ChallengerAgentID: challenger,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denial for mismatched challenger, got %+v", rpcErr)
	}
}
