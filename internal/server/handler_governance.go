package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type projectGovernancePredicatesCheckParams struct {
	WorkspaceID       string   `json:"workspace_id"`
	ProjectID         string   `json:"project_id"`
	ChallengedAgentID string   `json:"challenged_agent_id"`
	StallPredicates   []string `json:"stall_predicates"`
}

type projectGovernanceChallengeRaiseParams struct {
	WorkspaceID               string   `json:"workspace_id"`
	ProjectID                 string   `json:"project_id"`
	ActorID                   string   `json:"actor_id"`
	ChallengedAgentID         string   `json:"challenged_agent_id"`
	ChallengerAgentID         string   `json:"challenger_agent_id"`
	NominatedSuccessorAgentID string   `json:"nominated_successor_agent_id"`
	StallPredicates           []string `json:"stall_predicates"`
	EvidenceRefs              []string `json:"evidence_refs"`
	ArgumentDocKey            string   `json:"argument_doc_key"`
	TensionID                 string   `json:"tension_id"`
	DefenseWindowSeconds      int      `json:"defense_window_seconds"`
	VotingWindowSeconds       int      `json:"voting_window_seconds"`
	MaxRounds                 int      `json:"max_rounds"`
}

type projectGovernanceChallengeDefendParams struct {
	WorkspaceID         string `json:"workspace_id"`
	ActorID             string `json:"actor_id"`
	ChallengeID         string `json:"challenge_id"`
	Round               int    `json:"round"`
	Stance              string `json:"stance"`
	DefenseDocKey       string `json:"defense_doc_key"`
	VotingWindowSeconds int    `json:"voting_window_seconds"`
}

type projectGovernanceVoteCastParams struct {
	WorkspaceID     string `json:"workspace_id"`
	ActorID         string `json:"actor_id"`
	ChallengeID     string `json:"challenge_id"`
	Round           int    `json:"round"`
	VoterAgentID    string `json:"voter_agent_id"`
	Ballot          string `json:"ballot"`
	RationaleDocKey string `json:"rationale_doc_key"`
}

type projectGovernanceChallengeTallyParams struct {
	WorkspaceID      string `json:"workspace_id"`
	ActorID          string `json:"actor_id"`
	ChallengeID      string `json:"challenge_id"`
	ReassignEnabled  bool   `json:"reassign_enabled"`
	LeadLeaseSeconds int    `json:"lead_lease_seconds"`
}

type projectGovernanceChallengeGetParams struct {
	WorkspaceID  string `json:"workspace_id"`
	ChallengeID  string `json:"challenge_id"`
	IncludeVotes bool   `json:"include_votes"`
}

type projectGovernanceChallengeListParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	State       string `json:"state"`
	Limit       int    `json:"limit"`
}

type projectGovernanceVotesListParams struct {
	WorkspaceID string `json:"workspace_id"`
	ChallengeID string `json:"challenge_id"`
	Round       int    `json:"round"`
}

func (h *Handler) projectGovernancePredicatesCheck(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernancePredicatesCheckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	projectID := strings.TrimSpace(p.ProjectID)
	challengedAgentID := strings.TrimSpace(p.ChallengedAgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(challengedAgentID, "challenged_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	results, err := h.store.ValidateGovernanceStallPredicates(ctx, workspaceID, projectID, challengedAgentID, p.StallPredicates)
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.predicates.check")
	}
	return map[string]any{
		"predicate_results": results,
		"all_hold":          allGovernancePredicateResultsHold(results),
	}, nil
}

func (h *Handler) projectGovernanceChallengeRaise(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceChallengeRaiseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID, projectID, actorID, principal, rpcErr := h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	challengedAgentID := strings.TrimSpace(p.ChallengedAgentID)
	challengerAgentID := strings.TrimSpace(p.ChallengerAgentID)
	if rpcErr := requireTrimmedParam(challengedAgentID, "challenged_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(challengerAgentID, "challenger_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.EqualFold(principal.PrincipalType, "agent") && actorID != challengerAgentID {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "agent actor must match challenger_agent_id"}
	}
	challenge, event, err := h.store.RaiseGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeRaiseInput{
		WorkspaceID:               workspaceID,
		ProjectID:                 projectID,
		ActorID:                   actorID,
		ActorType:                 principal.PrincipalType,
		ChallengedAgentID:         challengedAgentID,
		ChallengerAgentID:         challengerAgentID,
		NominatedSuccessorAgentID: strings.TrimSpace(p.NominatedSuccessorAgentID),
		StallPredicates:           p.StallPredicates,
		EvidenceRefs:              p.EvidenceRefs,
		ArgumentDocKey:            strings.TrimSpace(p.ArgumentDocKey),
		TensionID:                 strings.TrimSpace(p.TensionID),
		DefenseWindowSeconds:      p.DefenseWindowSeconds,
		VotingWindowSeconds:       p.VotingWindowSeconds,
		MaxRounds:                 p.MaxRounds,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.governance.challenge.raise", map[string]string{
			"project_id":          projectID,
			"actor_id":            actorID,
			"challenged_agent_id": challengedAgentID,
			"challenger_agent_id": challengerAgentID,
		}),
		PromptContextSurface: "project.governance.challenge.raise",
	})
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.raise")
	}
	h.publishRuntimeEventRecord(event, projectID, challenge.ChallengeID, challengerAgentID)
	h.publishRuntimeEventRecordAs(event, "governance.challenge.changed", projectID, challenge.ChallengeID)
	return map[string]any{"challenge": challenge}, nil
}

func (h *Handler) projectGovernanceChallengeDefend(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceChallengeDefendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	challengeID := strings.TrimSpace(p.ChallengeID)
	if rpcErr := requireTrimmedParam(challengeID, "challenge_id"); rpcErr != nil {
		return nil, rpcErr
	}
	challenge, event, err := h.store.DefendGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeDefendInput{
		WorkspaceID:         workspaceID,
		ChallengeID:         challengeID,
		ActorID:             actorID,
		ActorType:           principal.PrincipalType,
		Round:               p.Round,
		Stance:              strings.TrimSpace(p.Stance),
		DefenseDocKey:       strings.TrimSpace(p.DefenseDocKey),
		VotingWindowSeconds: p.VotingWindowSeconds,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.governance.challenge.defend", map[string]string{
			"actor_id":     actorID,
			"challenge_id": challengeID,
		}),
		PromptContextSurface: "project.governance.challenge.defend",
	})
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.defend")
	}
	h.publishRuntimeEventRecord(event, challenge.ProjectID, challenge.ChallengeID)
	h.publishRuntimeEventRecordAs(event, "governance.challenge.changed", challenge.ProjectID, challenge.ChallengeID)
	return map[string]any{"challenge": challenge}, nil
}

func (h *Handler) projectGovernanceVoteCast(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceVoteCastParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	challengeID := strings.TrimSpace(p.ChallengeID)
	voterAgentID := strings.TrimSpace(p.VoterAgentID)
	if rpcErr := requireTrimmedParam(challengeID, "challenge_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(voterAgentID, "voter_agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	vote, event, err := h.store.VoteGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeVoteInput{
		WorkspaceID:     workspaceID,
		ChallengeID:     challengeID,
		ActorID:         actorID,
		ActorType:       principal.PrincipalType,
		VoterAgentID:    voterAgentID,
		Round:           p.Round,
		Ballot:          strings.TrimSpace(p.Ballot),
		RationaleDocKey: strings.TrimSpace(p.RationaleDocKey),
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.governance.vote.cast", map[string]string{
			"actor_id":       actorID,
			"challenge_id":   challengeID,
			"voter_agent_id": voterAgentID,
		}),
		PromptContextSurface: "project.governance.vote.cast",
	})
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.vote.cast")
	}
	h.publishRuntimeEventRecord(event, challengeID, voterAgentID, vote.Ballot)
	return map[string]any{"vote": vote}, nil
}

func (h *Handler) projectGovernanceChallengeTally(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceChallengeTallyParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	challengeID := strings.TrimSpace(p.ChallengeID)
	if rpcErr := requireTrimmedParam(challengeID, "challenge_id"); rpcErr != nil {
		return nil, rpcErr
	}
	result, err := h.store.TallyGovernanceChallengeWithEvent(ctx, sqlite.GovernanceChallengeTallyInput{
		WorkspaceID:      workspaceID,
		ChallengeID:      challengeID,
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		ReassignEnabled:  p.ReassignEnabled,
		LeadLeaseSeconds: p.LeadLeaseSeconds,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.governance.challenge.tally", map[string]string{
			"actor_id":     actorID,
			"challenge_id": challengeID,
		}),
		PromptContextSurface: "project.governance.challenge.tally",
	})
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.tally")
	}
	h.publishRuntimeEventRecord(result.Event, result.Challenge.ProjectID, result.Challenge.ChallengeID)
	h.publishRuntimeEventRecordAs(result.Event, "governance.challenge.changed", result.Challenge.ProjectID, result.Challenge.ChallengeID)
	if result.LeadTransferEvent != nil {
		h.publishRuntimeEventRecord(*result.LeadTransferEvent, result.Challenge.ProjectID, result.Challenge.ChallengeID)
		h.publishRuntimeEventRecordAs(*result.LeadTransferEvent, "project.lead.changed", result.Challenge.ProjectID, result.Challenge.ChallengeID)
	}
	return map[string]any{"tally": result}, nil
}

func (h *Handler) projectGovernanceChallengeGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceChallengeGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	challengeID := strings.TrimSpace(p.ChallengeID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(challengeID, "challenge_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	challenge, err := h.store.GetGovernanceChallenge(ctx, workspaceID, challengeID)
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.get")
	}
	response := map[string]any{"challenge": challenge}
	if p.IncludeVotes {
		votes, err := h.store.ListGovernanceVotes(ctx, workspaceID, challengeID, challenge.CurrentRound)
		if err != nil {
			return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.get")
		}
		response["votes"] = votes
	}
	return response, nil
}

func (h *Handler) projectGovernanceChallengeList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceChallengeListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	challenges, err := h.store.ListGovernanceChallenges(ctx, sqlite.GovernanceChallengeListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   strings.TrimSpace(p.ProjectID),
		State:       strings.TrimSpace(p.State),
		Limit:       p.Limit,
	})
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.challenge.list")
	}
	return map[string]any{"challenges": challenges, "count": len(challenges)}, nil
}

func (h *Handler) projectGovernanceVotesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGovernanceVotesListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	challengeID := strings.TrimSpace(p.ChallengeID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(challengeID, "challenge_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	votes, err := h.store.ListGovernanceVotes(ctx, workspaceID, challengeID, p.Round)
	if err != nil {
		return nil, rpcErrorFromGovernanceStore(err, "project.governance.votes.list")
	}
	return map[string]any{"votes": votes, "count": len(votes)}, nil
}

func rpcErrorFromGovernanceStore(err error, surface string) *RPCError {
	if rpcErr := authorityRejectRPCError(err, surface); rpcErr != nil {
		return rpcErr
	}
	switch {
	case errors.Is(err, sqlite.ErrGovernanceChallengeNotFound):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	case errors.Is(err, sqlite.ErrGovernancePredicateRejected),
		errors.Is(err, sqlite.ErrGovernanceChallengeStateInvalid),
		errors.Is(err, sqlite.ErrGovernanceVoteAlreadyCast),
		errors.Is(err, sqlite.ErrGovernanceInsufficientQuorum),
		errors.Is(err, sqlite.ErrGovernanceNominatedSuccessorEmpty),
		errors.Is(err, sqlite.ErrGovernanceReassignmentDisabled),
		errors.Is(err, sqlite.ErrProjectLeadMismatch),
		errors.Is(err, sqlite.ErrProjectLeadRequired):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}

func allGovernancePredicateResultsHold(results []sqlite.GovernanceStallPredicateResult) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		if !result.Holds {
			return false
		}
	}
	return true
}
