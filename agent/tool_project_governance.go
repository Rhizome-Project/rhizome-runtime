package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ProjectGovernanceTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectGovernanceTool(client *RhizomeClient, workspaceID, agentID string) *ProjectGovernanceTool {
	return &ProjectGovernanceTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func projectGovernanceToolsEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_ENABLE_PROJECT_GOVERNANCE_TOOLS")))
	return value == "1" || value == "true" || value == "yes"
}

func (t *ProjectGovernanceTool) Name() string { return "project_governance_challenge" }

func (t *ProjectGovernanceTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectGovernanceTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectGovernanceTool) Description() string {
	return "Governed project leadership challenge tool for strict fanout stalls. It can list challenges, check predicates, raise a challenge, defend, vote, and tally. Use only for objective project-level stalls, not ordinary disagreement."
}

func (t *ProjectGovernanceTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"description": "Action: list, check, raise, defend, vote, or tally.",
				"enum":        []string{"list", "check", "raise", "defend", "vote", "tally"},
			},
			"project_id": map[string]any{
				"type":        "string",
				"description": "Project id for list/check/raise.",
			},
			"state": map[string]any{
				"type":        "string",
				"description": "Optional challenge state filter for list.",
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Optional list limit.",
			},
			"challenge_id": map[string]any{
				"type":        "string",
				"description": "Challenge id for defend/vote/tally.",
			},
			"challenged_agent_id": map[string]any{
				"type":        "string",
				"description": "Current project strategic lead being challenged.",
			},
			"challenger_agent_id": map[string]any{
				"type":        "string",
				"description": "Agent raising the challenge. Defaults to this agent.",
			},
			"nominated_successor_agent_id": map[string]any{
				"type":        "string",
				"description": "Successor candidate if the vote reaches reassignment quorum.",
			},
			"stall_predicates": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string", "enum": []string{"fanout_absent", "idle_roster"}},
				"description": "Strict server-verified predicates. Defaults to fanout_absent and idle_roster.",
			},
			"evidence_refs": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Durable evidence refs or doc keys supporting a raise action.",
			},
			"argument_doc_key": map[string]any{
				"type":        "string",
				"description": "Workspace doc containing challenger arguments.",
			},
			"defense_doc_key": map[string]any{
				"type":        "string",
				"description": "Workspace doc containing lead defense arguments.",
			},
			"stance": map[string]any{
				"type":        "string",
				"description": "Defense stance: DEFEND or CONCEDE.",
				"enum":        []string{"DEFEND", "CONCEDE"},
			},
			"voter_agent_id": map[string]any{
				"type":        "string",
				"description": "Agent whose vote is being cast. Defaults to this agent.",
			},
			"ballot": map[string]any{
				"type":        "string",
				"description": "Vote ballot: UPHOLD_LEAD, REASSIGN, or ABSTAIN.",
				"enum":        []string{"UPHOLD_LEAD", "REASSIGN", "ABSTAIN"},
			},
			"rationale_doc_key": map[string]any{
				"type":        "string",
				"description": "Workspace doc with vote rationale.",
			},
			"round": map[string]any{
				"type":        "integer",
				"description": "Expected challenge round.",
			},
			"reassign_enabled": map[string]any{
				"type":        "boolean",
				"description": "Must be true to let tally transfer the strategic lead after a quorum.",
			},
		},
		"required": []string{"action"},
	}
}

func (t *ProjectGovernanceTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_governance_challenge is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	action := strings.ToLower(strings.TrimSpace(stringArg(args, "action")))
	switch action {
	case "list":
		return t.list(ctx, args)
	case "check":
		return t.check(ctx, args)
	case "raise":
		return t.raise(ctx, args)
	case "defend":
		return t.defend(ctx, args)
	case "vote":
		return t.vote(ctx, args)
	case "tally":
		return t.tally(ctx, args)
	default:
		return &ToolResult{Output: "action must be one of list, check, raise, defend, vote, or tally", IsError: true}
	}
}

func (t *ProjectGovernanceTool) list(ctx context.Context, args map[string]any) *ToolResult {
	projectID := projectToolRefArg(args, "project_id")
	if t.runtimeBinding != nil || projectID != "" {
		var err error
		projectID, err = resolveProjectToolActiveProjectID("project_governance_challenge list", t.workspaceID, projectID, t.runtimeBinding)
		if err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
	}
	challenges, err := t.client.ListProjectGovernanceChallenges(ctx, ProjectGovernanceChallengeListInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		State:       strings.TrimSpace(stringArg(args, "state")),
		Limit:       intArg(args["limit"], 0),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge list failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{
		"action":     "list",
		"count":      len(challenges),
		"challenges": challenges,
	})
}

func (t *ProjectGovernanceTool) check(ctx context.Context, args map[string]any) *ToolResult {
	projectID, err := resolveProjectToolActiveProjectID("project_governance_challenge check", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	challengedAgentID := strings.TrimSpace(stringArg(args, "challenged_agent_id"))
	if projectID == "" || challengedAgentID == "" {
		return &ToolResult{Output: "project_id and challenged_agent_id are required for check", IsError: true}
	}
	results, allHold, err := t.client.CheckProjectGovernancePredicates(ctx, ProjectGovernancePredicatesCheckInput{
		WorkspaceID:       t.workspaceID,
		ProjectID:         projectID,
		ChallengedAgentID: challengedAgentID,
		StallPredicates:   stringSliceArg(args, "stall_predicates"),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge check failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{
		"action":            "check",
		"project_id":        projectID,
		"challenged_agent":  challengedAgentID,
		"all_hold":          allHold,
		"predicate_results": results,
	})
}

func (t *ProjectGovernanceTool) raise(ctx context.Context, args map[string]any) *ToolResult {
	projectID, err := resolveProjectToolActiveProjectID("project_governance_challenge raise", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	challengedAgentID := strings.TrimSpace(stringArg(args, "challenged_agent_id"))
	challengerAgentID := firstNonEmpty(strings.TrimSpace(stringArg(args, "challenger_agent_id")), t.agentID)
	successorID := strings.TrimSpace(stringArg(args, "nominated_successor_agent_id"))
	if projectID == "" || challengedAgentID == "" {
		return &ToolResult{Output: "project_id and challenged_agent_id are required for raise", IsError: true}
	}
	results, allHold, err := t.client.CheckProjectGovernancePredicates(ctx, ProjectGovernancePredicatesCheckInput{
		WorkspaceID:       t.workspaceID,
		ProjectID:         projectID,
		ChallengedAgentID: challengedAgentID,
		StallPredicates:   stringSliceArg(args, "stall_predicates"),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge raise precheck failed: %v", err), IsError: true}
	}
	if !allHold {
		return governanceToolJSONError(map[string]any{
			"action":            "raise",
			"project_id":        projectID,
			"blocked":           "stall_predicates_not_satisfied",
			"predicate_results": results,
		})
	}
	challenge, err := t.client.RaiseProjectGovernanceChallenge(ctx, ProjectGovernanceChallengeRaiseInput{
		WorkspaceID:               t.workspaceID,
		ProjectID:                 projectID,
		ActorID:                   t.agentID,
		ChallengedAgentID:         challengedAgentID,
		ChallengerAgentID:         challengerAgentID,
		NominatedSuccessorAgentID: successorID,
		StallPredicates:           stringSliceArg(args, "stall_predicates"),
		EvidenceRefs:              stringSliceArg(args, "evidence_refs"),
		ArgumentDocKey:            strings.TrimSpace(stringArg(args, "argument_doc_key")),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge raise failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{
		"action":               "raise",
		"challenge":            challenge,
		"predicate_results":    results,
		"next_required_action": "challenged lead should call action=defend with DEFEND or CONCEDE; electorate can vote after defense opens voting",
	})
}

func (t *ProjectGovernanceTool) defend(ctx context.Context, args map[string]any) *ToolResult {
	challengeID := strings.TrimSpace(stringArg(args, "challenge_id"))
	stance := strings.ToUpper(strings.TrimSpace(stringArg(args, "stance")))
	if challengeID == "" || stance == "" {
		return &ToolResult{Output: "challenge_id and stance are required for defend", IsError: true}
	}
	challenge, err := t.client.DefendProjectGovernanceChallenge(ctx, ProjectGovernanceChallengeDefendInput{
		WorkspaceID:   t.workspaceID,
		ActorID:       t.agentID,
		ChallengeID:   challengeID,
		Round:         intArg(args["round"], 0),
		Stance:        stance,
		DefenseDocKey: strings.TrimSpace(stringArg(args, "defense_doc_key")),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge defend failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{"action": "defend", "challenge": challenge})
}

func (t *ProjectGovernanceTool) vote(ctx context.Context, args map[string]any) *ToolResult {
	challengeID := strings.TrimSpace(stringArg(args, "challenge_id"))
	ballot := strings.ToUpper(strings.TrimSpace(stringArg(args, "ballot")))
	voterID := firstNonEmpty(strings.TrimSpace(stringArg(args, "voter_agent_id")), t.agentID)
	if challengeID == "" || ballot == "" {
		return &ToolResult{Output: "challenge_id and ballot are required for vote", IsError: true}
	}
	vote, err := t.client.CastProjectGovernanceVote(ctx, ProjectGovernanceVoteCastInput{
		WorkspaceID:     t.workspaceID,
		ActorID:         t.agentID,
		ChallengeID:     challengeID,
		Round:           intArg(args["round"], 0),
		VoterAgentID:    voterID,
		Ballot:          ballot,
		RationaleDocKey: strings.TrimSpace(stringArg(args, "rationale_doc_key")),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge vote failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{"action": "vote", "vote": vote})
}

func (t *ProjectGovernanceTool) tally(ctx context.Context, args map[string]any) *ToolResult {
	challengeID := strings.TrimSpace(stringArg(args, "challenge_id"))
	if challengeID == "" {
		return &ToolResult{Output: "challenge_id is required for tally", IsError: true}
	}
	tally, err := t.client.TallyProjectGovernanceChallenge(ctx, ProjectGovernanceChallengeTallyInput{
		WorkspaceID:     t.workspaceID,
		ActorID:         t.agentID,
		ChallengeID:     challengeID,
		ReassignEnabled: boolArg(args, "reassign_enabled", false),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_governance_challenge tally failed: %v", err), IsError: true}
	}
	return governanceToolJSON(map[string]any{"action": "tally", "tally": tally})
}

func governanceToolJSON(payload map[string]any) *ToolResult {
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("%+v", payload)}
	}
	return &ToolResult{Output: string(raw)}
}

func governanceToolJSONError(payload map[string]any) *ToolResult {
	result := governanceToolJSON(payload)
	result.IsError = true
	return result
}
