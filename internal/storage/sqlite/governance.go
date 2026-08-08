package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	GovernanceChallengeStateRaised             = "RAISED"
	GovernanceChallengeStateDefenseOpen        = "DEFENSE_OPEN"
	GovernanceChallengeStateVoting             = "VOTING"
	GovernanceChallengeStateNegotiation        = "NEGOTIATION"
	GovernanceChallengeStateResolvedUpheld     = "RESOLVED_UPHELD"
	GovernanceChallengeStateResolvedReassigned = "RESOLVED_REASSIGNED"
	GovernanceChallengeStateResolvedDefault    = "RESOLVED_DEFAULT"
	GovernanceChallengeStateAutoWithdrawn      = "AUTO_WITHDRAWN"

	GovernanceBallotUpholdLead = "UPHOLD_LEAD"
	GovernanceBallotReassign   = "REASSIGN"
	GovernanceBallotAbstain    = "ABSTAIN"

	GovernancePredicateFanoutAbsent = "fanout_absent"
	GovernancePredicateIdleRoster   = "idle_roster"

	defaultGovernanceMaxRounds        = 3
	defaultGovernanceDefenseWindow    = 10 * time.Minute
	defaultGovernanceVotingWindow     = 10 * time.Minute
	governanceMinIdleAgentsForStall   = 2
	governanceDefaultLeadLeaseSeconds = 45 * 60
	governanceChallengeEntityType     = "governance_challenge"
)

var (
	ErrGovernanceChallengeNotFound       = errors.New("governance challenge not found")
	ErrGovernancePredicateRejected       = errors.New("governance stall predicates do not hold")
	ErrGovernanceChallengeStateInvalid   = errors.New("governance challenge state invalid for requested transition")
	ErrGovernanceVoteAlreadyCast         = errors.New("governance vote already cast for this round")
	ErrGovernanceInsufficientQuorum      = errors.New("governance vote has insufficient quorum")
	ErrGovernanceNominatedSuccessorEmpty = errors.New("governance nominated successor is required for reassignment")
	ErrGovernanceReassignmentDisabled    = errors.New("governance reassignment is disabled for this tally")
)

type GovernanceChallengeRecord struct {
	ChallengeID               string                           `json:"challenge_id"`
	WorkspaceID               string                           `json:"workspace_id"`
	ProjectID                 string                           `json:"project_id"`
	ChallengedAgentID         string                           `json:"challenged_agent_id"`
	ChallengerAgentID         string                           `json:"challenger_agent_id"`
	NominatedSuccessorAgentID string                           `json:"nominated_successor_agent_id,omitempty"`
	LeadRoleID                string                           `json:"lead_role_id,omitempty"`
	TensionID                 string                           `json:"tension_id,omitempty"`
	State                     string                           `json:"state"`
	CurrentRound              int                              `json:"current_round"`
	MaxRounds                 int                              `json:"max_rounds"`
	StallPredicates           []string                         `json:"stall_predicates,omitempty"`
	PredicateResults          []GovernanceStallPredicateResult `json:"predicate_results,omitempty"`
	EvidenceRefs              []string                         `json:"evidence_refs,omitempty"`
	ArgumentDocKey            string                           `json:"argument_doc_key,omitempty"`
	DefenseDocKey             string                           `json:"defense_doc_key,omitempty"`
	DefenseStance             string                           `json:"defense_stance,omitempty"`
	RoundOpenedAt             string                           `json:"round_opened_at"`
	DefenseDeadlineAt         string                           `json:"defense_deadline_at,omitempty"`
	VotingDeadlineAt          string                           `json:"voting_deadline_at,omitempty"`
	CreatedAt                 string                           `json:"created_at"`
	UpdatedAt                 string                           `json:"updated_at"`
	ResolvedAt                string                           `json:"resolved_at,omitempty"`
	Resolution                string                           `json:"resolution,omitempty"`
}

type GovernanceVoteRecord struct {
	VoteID          string `json:"vote_id"`
	ChallengeID     string `json:"challenge_id"`
	WorkspaceID     string `json:"workspace_id"`
	Round           int    `json:"round"`
	VoterAgentID    string `json:"voter_agent_id"`
	Ballot          string `json:"ballot"`
	RationaleDocKey string `json:"rationale_doc_key,omitempty"`
	CastAt          string `json:"cast_at"`
}

type GovernanceStallPredicateResult struct {
	Name         string   `json:"name"`
	Holds        bool     `json:"holds"`
	Summary      string   `json:"summary,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type GovernanceChallengeRaiseInput struct {
	WorkspaceID               string
	ProjectID                 string
	ChallengedAgentID         string
	ChallengerAgentID         string
	NominatedSuccessorAgentID string
	StallPredicates           []string
	EvidenceRefs              []string
	ArgumentDocKey            string
	TensionID                 string
	ActorID                   string
	ActorType                 string
	DefenseWindowSeconds      int
	VotingWindowSeconds       int
	MaxRounds                 int

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type GovernanceChallengeDefendInput struct {
	WorkspaceID         string
	ChallengeID         string
	Round               int
	ActorID             string
	ActorType           string
	Stance              string
	DefenseDocKey       string
	VotingWindowSeconds int

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type GovernanceChallengeVoteInput struct {
	WorkspaceID     string
	ChallengeID     string
	Round           int
	VoterAgentID    string
	Ballot          string
	RationaleDocKey string
	ActorID         string
	ActorType       string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type GovernanceChallengeTallyInput struct {
	WorkspaceID      string
	ChallengeID      string
	ActorID          string
	ActorType        string
	ReassignEnabled  bool
	ForceClose       bool
	LeadLeaseSeconds int

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type GovernanceChallengeTallyResult struct {
	Challenge         GovernanceChallengeRecord `json:"challenge"`
	Event             RuntimeEventRecord        `json:"event"`
	LeadRole          *ProjectRoleRecord        `json:"lead_role,omitempty"`
	LeadTransferEvent *RuntimeEventRecord       `json:"lead_transfer_event,omitempty"`
	ElectorateCount   int                       `json:"electorate_count"`
	QuorumThreshold   int                       `json:"quorum_threshold"`
	UpholdVotes       int                       `json:"uphold_votes"`
	ReassignVotes     int                       `json:"reassign_votes"`
	AbstainVotes      int                       `json:"abstain_votes"`
}

type GovernanceChallengeListFilter struct {
	WorkspaceID string
	ProjectID   string
	State       string
	Limit       int
}

type GovernanceDeadlineSweepResult struct {
	Scanned    int                              `json:"scanned"`
	Resolved   int                              `json:"resolved"`
	Failed     int                              `json:"failed"`
	Challenges []GovernanceChallengeTallyResult `json:"challenges,omitempty"`
}

func (s *Store) ValidateGovernanceStallPredicates(ctx context.Context, workspaceID, projectID, challengedAgentID string, predicates []string) ([]GovernanceStallPredicateResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	challengedAgentID = strings.TrimSpace(challengedAgentID)
	if workspaceID == "" || projectID == "" || challengedAgentID == "" {
		return nil, errors.New("workspace_id, project_id, and challenged_agent_id are required")
	}
	normalized, err := normalizeGovernancePredicates(predicates)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin governance predicate read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	results, err := s.governanceStallPredicateResultsTx(ctx, tx, workspaceID, projectID, challengedAgentID, normalized)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit governance predicate read tx: %w", err)
	}
	return results, nil
}

func (s *Store) RaiseGovernanceChallengeWithEvent(ctx context.Context, input GovernanceChallengeRaiseInput) (GovernanceChallengeRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	challengedAgentID := strings.TrimSpace(input.ChallengedAgentID)
	challengerAgentID := strings.TrimSpace(input.ChallengerAgentID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || challengedAgentID == "" || challengerAgentID == "" || actorID == "" {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, challenged_agent_id, challenger_agent_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	predicates, err := normalizeGovernancePredicates(input.StallPredicates)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, err
	}
	maxRounds := input.MaxRounds
	if maxRounds <= 0 {
		maxRounds = defaultGovernanceMaxRounds
	}
	if maxRounds < 1 || maxRounds > defaultGovernanceMaxRounds {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, errors.New("max_rounds must be between 1 and 3")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	defenseWindow := durationFromSeconds(input.DefenseWindowSeconds, defaultGovernanceDefenseWindow)
	votingWindow := durationFromSeconds(input.VotingWindowSeconds, defaultGovernanceVotingWindow)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin governance challenge tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var challenge GovernanceChallengeRecord
	var event RuntimeEventRecord
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	surface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "governance.challenge.raise")
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		lead, err := s.activeProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
		if err != nil {
			return err
		}
		if lead.AgentID != challengedAgentID {
			return ErrProjectLeadMismatch
		}
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, challengerAgentID); err != nil {
			return err
		}
		successorAgentID := strings.TrimSpace(input.NominatedSuccessorAgentID)
		if successorAgentID != "" {
			if successorAgentID == challengedAgentID {
				return errors.New("nominated successor cannot be the challenged lead")
			}
			if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, successorAgentID); err != nil {
				return err
			}
		}
		results, err := s.governanceStallPredicateResultsTx(ctx, tx, workspaceID, projectID, challengedAgentID, predicates)
		if err != nil {
			return err
		}
		if !allGovernancePredicatesHold(results) {
			return ErrGovernancePredicateRejected
		}
		challenge = GovernanceChallengeRecord{
			ChallengeID:               nextID("govchal"),
			WorkspaceID:               workspaceID,
			ProjectID:                 projectID,
			ChallengedAgentID:         challengedAgentID,
			ChallengerAgentID:         challengerAgentID,
			NominatedSuccessorAgentID: successorAgentID,
			LeadRoleID:                lead.RoleID,
			TensionID:                 strings.TrimSpace(input.TensionID),
			State:                     GovernanceChallengeStateDefenseOpen,
			CurrentRound:              1,
			MaxRounds:                 maxRounds,
			StallPredicates:           predicates,
			PredicateResults:          results,
			EvidenceRefs:              uniqueSortedStrings(append([]string(nil), input.EvidenceRefs...)),
			ArgumentDocKey:            strings.TrimSpace(input.ArgumentDocKey),
			RoundOpenedAt:             now,
			DefenseDeadlineAt:         nowTime.Add(defenseWindow).Format(time.RFC3339Nano),
			VotingDeadlineAt:          nowTime.Add(defenseWindow + votingWindow).Format(time.RFC3339Nano),
			CreatedAt:                 now,
			UpdatedAt:                 now,
		}
		if err := insertGovernanceChallengeTx(ctx, tx, challenge); err != nil {
			return err
		}
		payload := governanceChallengeEventPayload(challenge, "challenge.raise", actorID)
		var attachErr error
		payload, attachErr = attachProjectPromptContextEnvelope(payload, input.PromptContextEnvelope, surface, map[string]string{
			"workspace_id":              workspaceID,
			"project_id":                projectID,
			"actor_id":                  actorID,
			"principal_type":            actorType,
			"principal_id":              actorID,
			"challenge_id":              challenge.ChallengeID,
			"challenged_agent_id":       challengedAgentID,
			"challenger_agent_id":       challengerAgentID,
			"nominated_successor_agent": successorAgentID,
		})
		if attachErr != nil {
			return attachErr
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "governance.challenge.raised",
			EntityType:  governanceChallengeEntityType,
			EntityID:    challenge.ChallengeID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit governance challenge tx: %w", err)
	}
	return challenge, event, nil
}

func (s *Store) DefendGovernanceChallengeWithEvent(ctx context.Context, input GovernanceChallengeDefendInput) (GovernanceChallengeRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	challengeID := strings.TrimSpace(input.ChallengeID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || challengeID == "" || actorID == "" {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, challenge_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	stance, err := normalizeGovernanceDefenseStance(input.Stance)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, err
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	votingWindow := durationFromSeconds(input.VotingWindowSeconds, defaultGovernanceVotingWindow)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin governance defense tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var challenge GovernanceChallengeRecord
	var event RuntimeEventRecord
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	surface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "governance.challenge.defend")
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		current, err := s.getGovernanceChallengeTx(ctx, tx, workspaceID, challengeID)
		if err != nil {
			return err
		}
		if current.State != GovernanceChallengeStateDefenseOpen {
			return ErrGovernanceChallengeStateInvalid
		}
		if input.Round > 0 && input.Round != current.CurrentRound {
			return errors.New("round does not match current challenge round")
		}
		if actorID != current.ChallengedAgentID && strings.EqualFold(actorType, "agent") {
			return ErrProjectLeadMismatch
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE governance_challenges
   SET state = ?, defense_stance = ?, defense_doc_key = ?, voting_deadline_at = ?, resolved_at = ?, resolution = ?, updated_at = ?
 WHERE workspace_id = ? AND challenge_id = ?`,
			GovernanceChallengeStateVoting, stance, strings.TrimSpace(input.DefenseDocKey), nowTime.Add(votingWindow).Format(time.RFC3339Nano), "", "", now, workspaceID, challengeID); err != nil {
			return fmt.Errorf("update governance challenge defense: %w", err)
		}
		challenge, err = s.getGovernanceChallengeTx(ctx, tx, workspaceID, challengeID)
		if err != nil {
			return err
		}
		advisoryRecipients, err := s.appendGovernanceVoteOpenAdvisoriesTx(ctx, tx, challenge, now)
		if err != nil {
			return err
		}
		payload := governanceChallengeEventPayload(challenge, "challenge.defend", actorID)
		payload["advisory_recipient_count"] = advisoryRecipients
		var attachErr error
		payload, attachErr = attachProjectPromptContextEnvelope(payload, input.PromptContextEnvelope, surface, governancePromptContextFields(challenge, actorID, actorType))
		if attachErr != nil {
			return attachErr
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "governance.challenge.defended",
			EntityType:  governanceChallengeEntityType,
			EntityID:    challenge.ChallengeID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return GovernanceChallengeRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit governance defense tx: %w", err)
	}
	return challenge, event, nil
}

func (s *Store) VoteGovernanceChallengeWithEvent(ctx context.Context, input GovernanceChallengeVoteInput) (GovernanceVoteRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	challengeID := strings.TrimSpace(input.ChallengeID)
	voterAgentID := strings.TrimSpace(input.VoterAgentID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || challengeID == "" || voterAgentID == "" || actorID == "" {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, challenge_id, voter_agent_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	ballot, err := normalizeGovernanceBallot(input.Ballot)
	if err != nil {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin governance vote tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var vote GovernanceVoteRecord
	var event RuntimeEventRecord
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "agent")
	surface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "governance.challenge.vote")
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		challenge, err := s.getGovernanceChallengeTx(ctx, tx, workspaceID, challengeID)
		if err != nil {
			return err
		}
		if challenge.State != GovernanceChallengeStateVoting {
			return ErrGovernanceChallengeStateInvalid
		}
		if input.Round > 0 && input.Round != challenge.CurrentRound {
			return errors.New("round does not match current challenge round")
		}
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, voterAgentID); err != nil {
			return err
		}
		if strings.EqualFold(actorType, "agent") && actorID != voterAgentID {
			return errors.New("agent actor must match voter_agent_id")
		}
		vote = GovernanceVoteRecord{
			VoteID:          nextID("govvote"),
			ChallengeID:     challengeID,
			WorkspaceID:     workspaceID,
			Round:           challenge.CurrentRound,
			VoterAgentID:    voterAgentID,
			Ballot:          ballot,
			RationaleDocKey: strings.TrimSpace(input.RationaleDocKey),
			CastAt:          now,
		}
		if err := insertGovernanceVoteTx(ctx, tx, vote); err != nil {
			return err
		}
		payload := governanceVoteEventPayload(challenge, vote, "vote.cast", actorID)
		var attachErr error
		payload, attachErr = attachProjectPromptContextEnvelope(payload, input.PromptContextEnvelope, surface, governancePromptContextFields(challenge, actorID, actorType))
		if attachErr != nil {
			return attachErr
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "governance.vote.cast",
			EntityType:  "governance_vote",
			EntityID:    vote.VoteID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			err = ErrGovernanceVoteAlreadyCast
		}
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return GovernanceVoteRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit governance vote tx: %w", err)
	}
	return vote, event, nil
}

func (s *Store) TallyGovernanceChallengeWithEvent(ctx context.Context, input GovernanceChallengeTallyInput) (GovernanceChallengeTallyResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	challengeID := strings.TrimSpace(input.ChallengeID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || challengeID == "" || actorID == "" {
		return GovernanceChallengeTallyResult{}, errors.New("workspace_id, challenge_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return GovernanceChallengeTallyResult{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return GovernanceChallengeTallyResult{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return GovernanceChallengeTallyResult{}, fmt.Errorf("begin governance tally tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result := GovernanceChallengeTallyResult{}
	actorType := firstNonEmpty(strings.TrimSpace(input.ActorType), "human")
	surface := firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "governance.challenge.tally")
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		challenge, err := s.getGovernanceChallengeTx(ctx, tx, workspaceID, challengeID)
		if err != nil {
			return err
		}
		if challenge.State != GovernanceChallengeStateVoting {
			return ErrGovernanceChallengeStateInvalid
		}
		electorateCount, err := s.governanceElectorateCountTx(ctx, tx, workspaceID)
		if err != nil {
			return err
		}
		votes, err := s.listGovernanceVotesTx(ctx, tx, workspaceID, challengeID, challenge.CurrentRound)
		if err != nil {
			return err
		}
		upholdVotes, reassignVotes, abstainVotes := countGovernanceVotes(votes)
		threshold := governanceQuorumThreshold(electorateCount)
		result.ElectorateCount = electorateCount
		result.QuorumThreshold = threshold
		result.UpholdVotes = upholdVotes
		result.ReassignVotes = reassignVotes
		result.AbstainVotes = abstainVotes
		newState := GovernanceChallengeStateResolvedUpheld
		resolution := "incumbent upheld; reassign quorum not reached"
		var leadRole *ProjectRoleRecord
		var leadEvent *RuntimeEventRecord
		predicateResults, err := s.governanceStallPredicateResultsTx(ctx, tx, workspaceID, challenge.ProjectID, challenge.ChallengedAgentID, challenge.StallPredicates)
		if err != nil {
			return err
		}
		if !allGovernancePredicatesHold(predicateResults) {
			newState = GovernanceChallengeStateAutoWithdrawn
			resolution = "stall predicates cleared before tally"
		} else if reassignVotes >= threshold && reassignVotes > upholdVotes {
			if !input.ReassignEnabled {
				return ErrGovernanceReassignmentDisabled
			}
			if strings.TrimSpace(challenge.NominatedSuccessorAgentID) == "" {
				return ErrGovernanceNominatedSuccessorEmpty
			}
			newRole, transferEvent, err := s.governanceTransferProjectLeadTx(ctx, tx, authority, challenge, actorID, actorType, input.LeadLeaseSeconds, nowTime, input.PromptContextEnvelope, surface)
			if err != nil {
				return err
			}
			leadRole = &newRole
			leadEvent = &transferEvent
			newState = GovernanceChallengeStateResolvedReassigned
			resolution = "reassign quorum reached; project strategic lead transferred"
		} else if upholdVotes < threshold && (upholdVotes+reassignVotes+abstainVotes) < threshold {
			if input.ForceClose && governanceVotingDeadlineReached(challenge.VotingDeadlineAt, nowTime) {
				newState = GovernanceChallengeStateResolvedDefault
				resolution = "voting deadline reached with insufficient quorum; incumbent upheld by default"
			} else {
				return ErrGovernanceInsufficientQuorum
			}
		}
		predicateJSON := mustJSON(predicateResults)
		if _, err := tx.ExecContext(ctx, `
UPDATE governance_challenges
   SET state = ?, predicate_results_json = ?, resolved_at = ?, resolution = ?, updated_at = ?
 WHERE workspace_id = ? AND challenge_id = ?`,
			newState, predicateJSON, now, resolution, now, workspaceID, challengeID); err != nil {
			return fmt.Errorf("update governance tally: %w", err)
		}
		challenge, err = s.getGovernanceChallengeTx(ctx, tx, workspaceID, challengeID)
		if err != nil {
			return err
		}
		payload := governanceChallengeEventPayload(challenge, "challenge.tally", actorID)
		payload["electorate_count"] = electorateCount
		payload["quorum_threshold"] = threshold
		payload["uphold_votes"] = upholdVotes
		payload["reassign_votes"] = reassignVotes
		payload["abstain_votes"] = abstainVotes
		var attachErr error
		payload, attachErr = attachProjectPromptContextEnvelope(payload, input.PromptContextEnvelope, surface, governancePromptContextFields(challenge, actorID, actorType))
		if attachErr != nil {
			return attachErr
		}
		event, appendErr := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "governance.challenge.resolved",
			EntityType:  governanceChallengeEntityType,
			EntityID:    challenge.ChallengeID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		if appendErr != nil {
			return appendErr
		}
		result.Challenge = challenge
		result.Event = event
		result.LeadRole = leadRole
		result.LeadTransferEvent = leadEvent
		return nil
	}); err != nil {
		return GovernanceChallengeTallyResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return GovernanceChallengeTallyResult{}, fmt.Errorf("commit governance tally tx: %w", err)
	}
	return result, nil
}

func (s *Store) GetGovernanceChallenge(ctx context.Context, workspaceID, challengeID string) (GovernanceChallengeRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	challengeID = strings.TrimSpace(challengeID)
	if workspaceID == "" || challengeID == "" {
		return GovernanceChallengeRecord{}, errors.New("workspace_id and challenge_id are required")
	}
	return s.getGovernanceChallengeRead(ctx, workspaceID, challengeID)
}

func (s *Store) ListGovernanceChallenges(ctx context.Context, filter GovernanceChallengeListFilter) ([]GovernanceChallengeRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := `
SELECT challenge_id, workspace_id, project_id, challenged_agent_id, challenger_agent_id,
       nominated_successor_agent_id, lead_role_id, tension_id, state, current_round,
       max_rounds, stall_predicates_json, predicate_results_json, evidence_refs_json,
       argument_doc_key, defense_doc_key, defense_stance, round_opened_at,
       defense_deadline_at, voting_deadline_at, created_at, updated_at, resolved_at,
       resolution
  FROM governance_challenges
 WHERE workspace_id = ?`
	args := []any{workspaceID}
	if projectID := strings.TrimSpace(filter.ProjectID); projectID != "" {
		query += ` AND project_id = ?`
		args = append(args, projectID)
	}
	if state := strings.ToUpper(strings.TrimSpace(filter.State)); state != "" {
		query += ` AND state = ?`
		args = append(args, state)
	}
	query += ` ORDER BY updated_at DESC, challenge_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list governance challenges: %w", err)
	}
	defer rows.Close()
	var out []GovernanceChallengeRecord
	for rows.Next() {
		record, err := scanGovernanceChallenge(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance challenges: %w", err)
	}
	return out, nil
}

func (s *Store) ListGovernanceVotes(ctx context.Context, workspaceID, challengeID string, round int) ([]GovernanceVoteRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	challengeID = strings.TrimSpace(challengeID)
	if workspaceID == "" || challengeID == "" {
		return nil, errors.New("workspace_id and challenge_id are required")
	}
	if round <= 0 {
		round = 1
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT vote_id, challenge_id, workspace_id, round, voter_agent_id, ballot, rationale_doc_key, cast_at
  FROM governance_votes
 WHERE workspace_id = ? AND challenge_id = ? AND round = ?
 ORDER BY cast_at ASC, vote_id ASC`,
		workspaceID, challengeID, round)
	if err != nil {
		return nil, fmt.Errorf("list governance votes: %w", err)
	}
	defer rows.Close()
	var out []GovernanceVoteRecord
	for rows.Next() {
		vote, err := scanGovernanceVote(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, vote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance votes: %w", err)
	}
	return out, nil
}

func (s *Store) SweepExpiredGovernanceChallengesWithEvent(ctx context.Context, limit int) (GovernanceDeadlineSweepResult, error) {
	if limit <= 0 || limit > 100 {
		limit = 32
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, challenge_id
  FROM governance_challenges
 WHERE state = ?
   AND TRIM(COALESCE(voting_deadline_at, '')) <> ''
   AND voting_deadline_at <= ?
 ORDER BY voting_deadline_at ASC, challenge_id ASC
 LIMIT ?`,
		GovernanceChallengeStateVoting, now, limit)
	if err != nil {
		return GovernanceDeadlineSweepResult{}, fmt.Errorf("list expired governance challenges: %w", err)
	}
	defer rows.Close()

	type expiredChallenge struct {
		workspaceID string
		challengeID string
	}
	var expired []expiredChallenge
	for rows.Next() {
		var item expiredChallenge
		if err := rows.Scan(&item.workspaceID, &item.challengeID); err != nil {
			return GovernanceDeadlineSweepResult{}, fmt.Errorf("scan expired governance challenge: %w", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		return GovernanceDeadlineSweepResult{}, fmt.Errorf("iterate expired governance challenges: %w", err)
	}

	result := GovernanceDeadlineSweepResult{Scanned: len(expired)}
	var firstErr error
	for _, item := range expired {
		tally, err := s.TallyGovernanceChallengeWithEvent(ctx, GovernanceChallengeTallyInput{
			WorkspaceID:      item.workspaceID,
			ChallengeID:      item.challengeID,
			ActorID:          "governance_deadline_loop",
			ActorType:        "system",
			ReassignEnabled:  true,
			ForceClose:       true,
			LeadLeaseSeconds: governanceDefaultLeadLeaseSeconds,
			PromptContextEnvelope: BuildProjectPromptContextEnvelope(
				"project.governance.challenge.deadline_sweep",
				"serve_loop",
				item.workspaceID,
				"system",
				"governance_deadline_loop",
			),
			PromptContextSurface: "project.governance.challenge.deadline_sweep",
		})
		if err != nil {
			result.Failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("sweep governance challenge %s: %w", item.challengeID, err)
			}
			continue
		}
		result.Resolved++
		result.Challenges = append(result.Challenges, tally)
	}
	return result, firstErr
}

func (s *Store) governanceTransferProjectLeadTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, challenge GovernanceChallengeRecord, actorID, actorType string, leaseSeconds int, nowTime time.Time, envelope map[string]any, surface string) (ProjectRoleRecord, RuntimeEventRecord, error) {
	now := nowTime.Format(time.RFC3339Nano)
	role, err := s.getProjectRoleTx(ctx, tx, challenge.WorkspaceID, challenge.ProjectID, challenge.LeadRoleID)
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	if role.RoleType != ProjectRoleStrategicLead || role.AgentID != challenge.ChallengedAgentID || !projectRoleLeaseActiveAt(role, now) {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, ErrProjectLeadRequired
	}
	if err := s.ensureProjectRoleScopeTx(ctx, tx, challenge.WorkspaceID, challenge.ProjectID, challenge.NominatedSuccessorAgentID); err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	if leaseSeconds <= 0 {
		leaseSeconds = governanceDefaultLeadLeaseSeconds
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, released_at = ?, summary = ?, updated_by = ?, updated_at = ?
 WHERE role_id = ?`,
		ProjectRoleStatusReleased, now, firstNonEmpty(strings.TrimSpace(role.Summary), "released by governance challenge "+challenge.ChallengeID), actorID, now, role.RoleID); err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, fmt.Errorf("release challenged project lead: %w", err)
	}
	next := ProjectRoleRecord{
		RoleID:         nextID("projrole"),
		WorkspaceID:    challenge.WorkspaceID,
		ProjectID:      challenge.ProjectID,
		AgentID:        challenge.NominatedSuccessorAgentID,
		RoleType:       ProjectRoleStrategicLead,
		Status:         ProjectRoleStatusActive,
		WriteScopeJSON: "{}",
		LeaseToken:     nextID("projleadlease"),
		LeaseExpiresAt: projectLeadLeaseUntil(nowTime, leaseSeconds),
		Summary:        "governance challenge " + challenge.ChallengeID + " reassigned project strategic lead",
		ClaimedAt:      now,
		UpdatedBy:      actorID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := insertProjectRoleTx(ctx, tx, next); err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	payload := projectRoleEventPayload(next, "lead.governance_transfer", actorID)
	payload["governance_challenge_id"] = challenge.ChallengeID
	payload["previous_role_id"] = role.RoleID
	payload["previous_agent_id"] = role.AgentID
	var attachErr error
	payload, attachErr = attachProjectPromptContextEnvelope(payload, envelope, surface, map[string]string{
		"workspace_id":        challenge.WorkspaceID,
		"project_id":          challenge.ProjectID,
		"actor_id":            actorID,
		"agent_id":            next.AgentID,
		"role_id":             next.RoleID,
		"principal_type":      actorType,
		"principal_id":        actorID,
		"challenge_id":        challenge.ChallengeID,
		"challenged_agent_id": challenge.ChallengedAgentID,
	})
	if attachErr != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, attachErr
	}
	event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID: challenge.WorkspaceID,
		EventType:   "project.lead.transferred",
		EntityType:  "project_role",
		EntityID:    next.RoleID,
		ActorType:   actorType,
		ActorID:     actorID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	return next, event, nil
}

func (s *Store) governanceStallPredicateResultsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, challengedAgentID string, predicates []string) ([]GovernanceStallPredicateResult, error) {
	results := make([]GovernanceStallPredicateResult, 0, len(predicates))
	for _, predicate := range predicates {
		switch predicate {
		case GovernancePredicateFanoutAbsent:
			result, err := s.governanceFanoutAbsentTx(ctx, tx, workspaceID, projectID)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		case GovernancePredicateIdleRoster:
			result, err := s.governanceIdleRosterTx(ctx, tx, workspaceID, challengedAgentID)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		default:
			return nil, fmt.Errorf("unsupported governance stall predicate %q", predicate)
		}
	}
	return results, nil
}

func (s *Store) governanceFanoutAbsentTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (GovernanceStallPredicateResult, error) {
	profile, err := s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return GovernanceStallPredicateResult{}, err
	}
	var openImplementationTasks int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE wt.workspace_id = ?
   AND t.project_id = ?
   AND UPPER(TRIM(COALESCE(t.status, ''))) NOT IN (?, ?, ?)
   AND LOWER(TRIM(COALESCE(t.project_lane, ''))) IN ('implementation', 'implement', 'coding', 'code', 'frontend', 'front-end', 'ui', 'backend', 'back-end', 'api', 'fullstack', 'full-stack')`,
		workspaceID, projectID, model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled).Scan(&openImplementationTasks); err != nil {
		return GovernanceStallPredicateResult{}, fmt.Errorf("count open implementation tasks: %w", err)
	}
	phaseOpen := projectPhaseAllowsImplementationWork(profile.CurrentPhase)
	holds := phaseOpen && openImplementationTasks == 0
	summary := fmt.Sprintf("project phase=%s, open implementation tasks=%d", strings.TrimSpace(profile.CurrentPhase), openImplementationTasks)
	return GovernanceStallPredicateResult{
		Name:         GovernancePredicateFanoutAbsent,
		Holds:        holds,
		Summary:      summary,
		EvidenceRefs: []string{"project:" + projectID, "phase:" + strings.TrimSpace(profile.CurrentPhase), fmt.Sprintf("open_implementation_tasks:%d", openImplementationTasks)},
	}, nil
}

func (s *Store) governanceIdleRosterTx(ctx context.Context, tx *sql.Tx, workspaceID, challengedAgentID string) (GovernanceStallPredicateResult, error) {
	var idleAgents int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM agents a
 WHERE a.workspace_id = ?
   AND a.agent_id <> ?
   AND UPPER(TRIM(COALESCE(a.status, ''))) NOT IN ('OFFLINE', 'PAUSED')
   AND NOT EXISTS (
     SELECT 1
       FROM task_claims c
      WHERE c.workspace_id = a.workspace_id
        AND c.agent_id = a.agent_id
        AND c.claim_status IN ('CLAIMED', 'BLOCKED')
   )`,
		workspaceID, challengedAgentID).Scan(&idleAgents); err != nil {
		return GovernanceStallPredicateResult{}, fmt.Errorf("count idle governance roster: %w", err)
	}
	holds := idleAgents >= governanceMinIdleAgentsForStall
	return GovernanceStallPredicateResult{
		Name:         GovernancePredicateIdleRoster,
		Holds:        holds,
		Summary:      fmt.Sprintf("idle non-lead agents=%d, required=%d", idleAgents, governanceMinIdleAgentsForStall),
		EvidenceRefs: []string{fmt.Sprintf("idle_non_lead_agents:%d", idleAgents), fmt.Sprintf("required_idle_agents:%d", governanceMinIdleAgentsForStall)},
	}, nil
}

func (s *Store) governanceElectorateCountTx(ctx context.Context, tx *sql.Tx, workspaceID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM agents
 WHERE workspace_id = ?
   AND UPPER(TRIM(COALESCE(status, ''))) NOT IN ('OFFLINE', 'PAUSED')`,
		workspaceID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count governance electorate: %w", err)
	}
	return count, nil
}

func (s *Store) governanceElectorateAgentIDsTx(ctx context.Context, tx *sql.Tx, workspaceID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT agent_id
  FROM agents
 WHERE workspace_id = ?
   AND UPPER(TRIM(COALESCE(status, ''))) NOT IN ('OFFLINE', 'PAUSED')
 ORDER BY agent_id`,
		workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list governance electorate agents: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan governance electorate agent: %w", err)
		}
		if agentID = strings.TrimSpace(agentID); agentID != "" {
			out = append(out, agentID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance electorate agents: %w", err)
	}
	return out, nil
}

func (s *Store) appendGovernanceVoteOpenAdvisoriesTx(ctx context.Context, tx *sql.Tx, challenge GovernanceChallengeRecord, now string) (int, error) {
	agentIDs, err := s.governanceElectorateAgentIDsTx(ctx, tx, challenge.WorkspaceID)
	if err != nil {
		return 0, err
	}
	signal := governanceVoteOpenAdvisorySignal(challenge)
	for _, agentID := range agentIDs {
		var previous sql.NullString
		err := tx.QueryRowContext(ctx,
			`SELECT state_value FROM agent_state WHERE workspace_id = ? AND agent_id = ? AND state_key = ?`,
			challenge.WorkspaceID, agentID, runtimeScratchAgentStateKey,
		).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("get governance advisory scratch for %s: %w", agentID, err)
		}
		next, changed := appendGovernanceScratchAdvisory(previous.String, signal, challenge.ChallengeID)
		if !changed {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO agent_state (workspace_id, agent_id, state_key, state_value, updated_at)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(workspace_id, agent_id, state_key) DO UPDATE SET
			   state_value = excluded.state_value,
			   updated_at = excluded.updated_at`,
			challenge.WorkspaceID, agentID, runtimeScratchAgentStateKey, next, now,
		); err != nil {
			return 0, fmt.Errorf("set governance advisory scratch for %s: %w", agentID, err)
		}
	}
	return len(agentIDs), nil
}

func (s *Store) listGovernanceVotesTx(ctx context.Context, tx *sql.Tx, workspaceID, challengeID string, round int) ([]GovernanceVoteRecord, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT vote_id, challenge_id, workspace_id, round, voter_agent_id, ballot, rationale_doc_key, cast_at
  FROM governance_votes
 WHERE workspace_id = ? AND challenge_id = ? AND round = ?
 ORDER BY cast_at ASC, vote_id ASC`,
		workspaceID, challengeID, round)
	if err != nil {
		return nil, fmt.Errorf("list governance votes tx: %w", err)
	}
	defer rows.Close()
	var out []GovernanceVoteRecord
	for rows.Next() {
		vote, err := scanGovernanceVote(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, vote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance votes tx: %w", err)
	}
	return out, nil
}

func insertGovernanceChallengeTx(ctx context.Context, tx *sql.Tx, record GovernanceChallengeRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO governance_challenges (
  challenge_id, workspace_id, project_id, challenged_agent_id, challenger_agent_id,
  nominated_successor_agent_id, lead_role_id, tension_id, state, current_round,
  max_rounds, stall_predicates_json, predicate_results_json, evidence_refs_json,
  argument_doc_key, defense_doc_key, defense_stance, round_opened_at,
  defense_deadline_at, voting_deadline_at, created_at, updated_at, resolved_at,
  resolution
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ChallengeID, record.WorkspaceID, record.ProjectID, record.ChallengedAgentID, record.ChallengerAgentID,
		record.NominatedSuccessorAgentID, record.LeadRoleID, record.TensionID, record.State, record.CurrentRound,
		record.MaxRounds, mustJSON(record.StallPredicates), mustJSON(record.PredicateResults), mustJSON(record.EvidenceRefs),
		record.ArgumentDocKey, record.DefenseDocKey, record.DefenseStance, record.RoundOpenedAt,
		record.DefenseDeadlineAt, record.VotingDeadlineAt, record.CreatedAt, record.UpdatedAt, record.ResolvedAt,
		record.Resolution); err != nil {
		return fmt.Errorf("insert governance challenge: %w", err)
	}
	return nil
}

func insertGovernanceVoteTx(ctx context.Context, tx *sql.Tx, record GovernanceVoteRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO governance_votes (
  vote_id, challenge_id, workspace_id, round, voter_agent_id, ballot, rationale_doc_key, cast_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.VoteID, record.ChallengeID, record.WorkspaceID, record.Round, record.VoterAgentID, record.Ballot, record.RationaleDocKey, record.CastAt); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return ErrGovernanceVoteAlreadyCast
		}
		return fmt.Errorf("insert governance vote: %w", err)
	}
	return nil
}

func (s *Store) getGovernanceChallengeRead(ctx context.Context, workspaceID, challengeID string) (GovernanceChallengeRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT challenge_id, workspace_id, project_id, challenged_agent_id, challenger_agent_id,
       nominated_successor_agent_id, lead_role_id, tension_id, state, current_round,
       max_rounds, stall_predicates_json, predicate_results_json, evidence_refs_json,
       argument_doc_key, defense_doc_key, defense_stance, round_opened_at,
       defense_deadline_at, voting_deadline_at, created_at, updated_at, resolved_at,
       resolution
  FROM governance_challenges
 WHERE workspace_id = ? AND challenge_id = ?`,
		workspaceID, challengeID)
	return scanGovernanceChallenge(row)
}

func (s *Store) getGovernanceChallengeTx(ctx context.Context, tx *sql.Tx, workspaceID, challengeID string) (GovernanceChallengeRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT challenge_id, workspace_id, project_id, challenged_agent_id, challenger_agent_id,
       nominated_successor_agent_id, lead_role_id, tension_id, state, current_round,
       max_rounds, stall_predicates_json, predicate_results_json, evidence_refs_json,
       argument_doc_key, defense_doc_key, defense_stance, round_opened_at,
       defense_deadline_at, voting_deadline_at, created_at, updated_at, resolved_at,
       resolution
  FROM governance_challenges
 WHERE workspace_id = ? AND challenge_id = ?`,
		workspaceID, challengeID)
	return scanGovernanceChallenge(row)
}

type governanceChallengeScanner interface {
	Scan(dest ...any) error
}

func scanGovernanceChallenge(row governanceChallengeScanner) (GovernanceChallengeRecord, error) {
	var record GovernanceChallengeRecord
	var predicatesJSON, predicateResultsJSON, evidenceRefsJSON string
	if err := row.Scan(
		&record.ChallengeID,
		&record.WorkspaceID,
		&record.ProjectID,
		&record.ChallengedAgentID,
		&record.ChallengerAgentID,
		&record.NominatedSuccessorAgentID,
		&record.LeadRoleID,
		&record.TensionID,
		&record.State,
		&record.CurrentRound,
		&record.MaxRounds,
		&predicatesJSON,
		&predicateResultsJSON,
		&evidenceRefsJSON,
		&record.ArgumentDocKey,
		&record.DefenseDocKey,
		&record.DefenseStance,
		&record.RoundOpenedAt,
		&record.DefenseDeadlineAt,
		&record.VotingDeadlineAt,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.ResolvedAt,
		&record.Resolution,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GovernanceChallengeRecord{}, ErrGovernanceChallengeNotFound
		}
		return GovernanceChallengeRecord{}, fmt.Errorf("scan governance challenge: %w", err)
	}
	record.StallPredicates = decodeStringJSONArray(predicatesJSON)
	record.EvidenceRefs = decodeStringJSONArray(evidenceRefsJSON)
	var predicateResults []GovernanceStallPredicateResult
	if err := unmarshalJSONString(predicateResultsJSON, &predicateResults); err == nil {
		record.PredicateResults = predicateResults
	}
	return record, nil
}

type governanceVoteScanner interface {
	Scan(dest ...any) error
}

func scanGovernanceVote(row governanceVoteScanner) (GovernanceVoteRecord, error) {
	var vote GovernanceVoteRecord
	if err := row.Scan(
		&vote.VoteID,
		&vote.ChallengeID,
		&vote.WorkspaceID,
		&vote.Round,
		&vote.VoterAgentID,
		&vote.Ballot,
		&vote.RationaleDocKey,
		&vote.CastAt,
	); err != nil {
		return GovernanceVoteRecord{}, fmt.Errorf("scan governance vote: %w", err)
	}
	return vote, nil
}

func unmarshalJSONString(raw string, out any) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "[]"
	}
	return jsonUnmarshalString(raw, out)
}

func jsonUnmarshalString(raw string, out any) error {
	return json.Unmarshal([]byte(raw), out)
}

func normalizeGovernancePredicates(values []string) ([]string, error) {
	if len(values) == 0 {
		values = []string{GovernancePredicateFanoutAbsent, GovernancePredicateIdleRoster}
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		predicate := strings.ToLower(strings.TrimSpace(value))
		switch predicate {
		case GovernancePredicateFanoutAbsent, GovernancePredicateIdleRoster:
		default:
			return nil, fmt.Errorf("unsupported governance stall predicate %q", value)
		}
		if _, ok := seen[predicate]; ok {
			continue
		}
		seen[predicate] = struct{}{}
		out = append(out, predicate)
	}
	return out, nil
}

func normalizeGovernanceDefenseStance(value string) (string, error) {
	stance := strings.ToUpper(strings.TrimSpace(value))
	switch stance {
	case "DEFEND", "CONCEDE":
		return stance, nil
	default:
		return "", errors.New("defense stance must be DEFEND or CONCEDE")
	}
}

func normalizeGovernanceBallot(value string) (string, error) {
	ballot := strings.ToUpper(strings.TrimSpace(value))
	switch ballot {
	case GovernanceBallotUpholdLead, GovernanceBallotReassign, GovernanceBallotAbstain:
		return ballot, nil
	default:
		return "", errors.New("ballot must be UPHOLD_LEAD, REASSIGN, or ABSTAIN")
	}
}

func allGovernancePredicatesHold(results []GovernanceStallPredicateResult) bool {
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

func countGovernanceVotes(votes []GovernanceVoteRecord) (int, int, int) {
	var uphold, reassign, abstain int
	for _, vote := range votes {
		switch strings.ToUpper(strings.TrimSpace(vote.Ballot)) {
		case GovernanceBallotReassign:
			reassign++
		case GovernanceBallotAbstain:
			abstain++
		default:
			uphold++
		}
	}
	return uphold, reassign, abstain
}

func governanceQuorumThreshold(electorateCount int) int {
	if electorateCount <= 0 {
		return 1
	}
	return (2*electorateCount + 2) / 3
}

func governanceVotingDeadlineReached(deadline string, now time.Time) bool {
	deadline = strings.TrimSpace(deadline)
	if deadline == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, deadline)
		if err == nil {
			return !now.UTC().Before(parsed.UTC())
		}
	}
	return false
}

func governanceVoteOpenAdvisorySignal(challenge GovernanceChallengeRecord) string {
	parts := []string{
		fmt.Sprintf("GOVERNANCE VOTE OPEN: challenge %s for project %s is in VOTING round %d.", strings.TrimSpace(challenge.ChallengeID), strings.TrimSpace(challenge.ProjectID), challenge.CurrentRound),
		"Use project_governance_challenge action=list to inspect active challenges, then cast one vote with action=vote ballot=UPHOLD_LEAD|REASSIGN|ABSTAIN.",
	}
	if doc := strings.TrimSpace(challenge.ArgumentDocKey); doc != "" {
		parts = append(parts, "Argument doc: "+doc+".")
	}
	if doc := strings.TrimSpace(challenge.DefenseDocKey); doc != "" {
		parts = append(parts, "Defense doc: "+doc+".")
	}
	if successor := strings.TrimSpace(challenge.NominatedSuccessorAgentID); successor != "" {
		parts = append(parts, "Reassign candidate: "+successor+".")
	}
	if deadline := strings.TrimSpace(challenge.VotingDeadlineAt); deadline != "" {
		parts = append(parts, "Voting deadline: "+deadline+".")
	}
	return strings.Join(parts, " ")
}

func appendGovernanceScratchAdvisory(raw, signal, challengeID string) (string, bool) {
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return strings.TrimSpace(raw), false
	}
	var state map[string]any
	if raw = strings.TrimSpace(raw); raw != "" {
		_ = json.Unmarshal([]byte(raw), &state)
	}
	if state == nil {
		state = map[string]any{}
	}
	signals := governanceScratchAdvisorySignals(state["advisory_signals"])
	if governanceScratchHasChallengeAdvisory(signals, challengeID, signal) {
		next, _ := json.Marshal(state)
		return string(next), false
	}
	signals = append(signals, signal)
	if len(signals) > 5 {
		signals = signals[len(signals)-5:]
	}
	state["advisory_signals"] = signals
	if _, ok := state["doc_shas"]; !ok {
		state["doc_shas"] = map[string]string{}
	}
	next, err := json.Marshal(state)
	if err != nil {
		return strings.TrimSpace(raw), false
	}
	return string(next), true
}

func governanceScratchAdvisorySignals(value any) []string {
	switch typed := value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				if text = strings.TrimSpace(text); text != "" {
					out = append(out, text)
				}
			}
		}
		return out
	default:
		return nil
	}
}

func governanceScratchHasChallengeAdvisory(signals []string, challengeID, signal string) bool {
	challengeID = strings.TrimSpace(challengeID)
	signal = strings.TrimSpace(signal)
	for _, existing := range signals {
		existing = strings.TrimSpace(existing)
		if existing == "" {
			continue
		}
		if existing == signal {
			return true
		}
		if challengeID != "" && strings.Contains(existing, "GOVERNANCE VOTE OPEN: challenge "+challengeID) {
			return true
		}
	}
	return false
}

func durationFromSeconds(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func governanceChallengeEventPayload(challenge GovernanceChallengeRecord, operation, actorID string) map[string]any {
	return map[string]any{
		"workspace_id":                 challenge.WorkspaceID,
		"project_id":                   challenge.ProjectID,
		"challenge_id":                 challenge.ChallengeID,
		"challenged_agent_id":          challenge.ChallengedAgentID,
		"challenger_agent_id":          challenge.ChallengerAgentID,
		"nominated_successor_agent_id": challenge.NominatedSuccessorAgentID,
		"lead_role_id":                 challenge.LeadRoleID,
		"state":                        challenge.State,
		"current_round":                challenge.CurrentRound,
		"max_rounds":                   challenge.MaxRounds,
		"stall_predicates":             challenge.StallPredicates,
		"predicate_results":            challenge.PredicateResults,
		"evidence_refs":                challenge.EvidenceRefs,
		"argument_doc_key":             challenge.ArgumentDocKey,
		"defense_doc_key":              challenge.DefenseDocKey,
		"defense_stance":               challenge.DefenseStance,
		"voting_deadline_at":           challenge.VotingDeadlineAt,
		"actor_id":                     strings.TrimSpace(actorID),
		"entity_type":                  governanceChallengeEntityType,
		"entity_id":                    challenge.ChallengeID,
		"summary":                      "Governance challenge " + challenge.State + ": " + challenge.ProjectID,
		"mutation_operation":           operation,
		"challenge":                    challenge,
	}
}

func governanceVoteEventPayload(challenge GovernanceChallengeRecord, vote GovernanceVoteRecord, operation, actorID string) map[string]any {
	payload := governanceChallengeEventPayload(challenge, operation, actorID)
	payload["vote_id"] = vote.VoteID
	payload["voter_agent_id"] = vote.VoterAgentID
	payload["ballot"] = vote.Ballot
	payload["round"] = vote.Round
	payload["vote"] = vote
	return payload
}

func governancePromptContextFields(challenge GovernanceChallengeRecord, actorID, actorType string) map[string]string {
	return map[string]string{
		"workspace_id":                 challenge.WorkspaceID,
		"project_id":                   challenge.ProjectID,
		"actor_id":                     actorID,
		"principal_type":               actorType,
		"principal_id":                 actorID,
		"challenge_id":                 challenge.ChallengeID,
		"challenged_agent_id":          challenge.ChallengedAgentID,
		"challenger_agent_id":          challenge.ChallengerAgentID,
		"nominated_successor_agent_id": challenge.NominatedSuccessorAgentID,
	}
}
