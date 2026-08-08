package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const projectClaimRepairLeadStaleAfter = 15 * time.Minute

func agentProfileAllowsAutonomousExecution(profile AgentProfileRecord) bool {
	allowed, _, _ := agentProfileAutonomousExecutionGate(profile)
	return allowed
}

func agentProfileAutonomousExecutionGate(profile AgentProfileRecord) (bool, string, string) {
	specialization := strings.ToLower(strings.TrimSpace(profile.Specialization))
	bio := strings.ToLower(strings.TrimSpace(profile.Bio))
	mode := strings.ToLower(strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "default_work_mode")))

	if strings.Contains(specialization, "meta-analysis") || strings.Contains(specialization, "meta analysis") {
		return false, "specialization_meta_analysis", "Agent profile specialization marks this agent as meta-analysis/observer work."
	}
	if strings.Contains(mode, "observer") {
		return false, "default_work_mode_observer", "Agent profile default_work_mode is observer."
	}
	for _, tag := range profile.Tags {
		tagValue := strings.ToLower(strings.TrimSpace(tag))
		if strings.Contains(tagValue, "observer") {
			return false, "tag_observer", "Agent profile tags mark this agent as observer."
		}
		if strings.Contains(tagValue, "meta-analysis") || strings.Contains(tagValue, "meta analysis") {
			return false, "tag_meta_analysis", "Agent profile tags mark this agent as meta-analysis."
		}
	}
	if strings.Contains(bio, "without direct participation") {
		return false, "bio_without_direct_participation", "Agent profile bio says the agent works without direct participation."
	}
	if strings.Contains(bio, "do not solve problems") {
		return false, "bio_do_not_solve_problems", "Agent profile bio says the agent should not solve problems directly."
	}
	return true, "profile_allows_autonomous_execution", "Agent profile allows autonomous work selection."
}

func agentProfileMetadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[strings.TrimSpace(key)]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func agentProfileMetadataBool(metadata map[string]any, key string) (bool, bool) {
	if len(metadata) == 0 {
		return false, false
	}
	value, ok := metadata[strings.TrimSpace(key)]
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "y", "on":
			return true, true
		case "false", "0", "no", "n", "off":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

type AgentWorkNextFilter struct {
	WorkspaceID        string
	AgentID            string
	IncludeHydration   bool
	IncludePacket      bool
	IncludeAdvisory    bool
	EnableTaskFrontier bool
	FrontierLimit      int
	DocKeys            []string
	IncludeAllDocs     bool
	UpdatesLimit       int
	ArtifactLimit      int
	RelatedTaskLimit   int
	SessionLimit       int
	Trigger            string
	CandidateTaskID    string
	CandidateSessionID string
	CoordinationMode   string
}

type AgentWorkNextResult struct {
	GeneratedAt                string                   `json:"generated_at"`
	TimeAuthority              WorkspaceTimeAuthority   `json:"time_authority"`
	WorkspaceID                string                   `json:"workspace_id"`
	AgentID                    string                   `json:"agent_id"`
	HasWork                    bool                     `json:"has_work"`
	Reason                     string                   `json:"reason"`
	AutonomousExecutionAllowed bool                     `json:"autonomous_execution_allowed"`
	ProfileGateReason          string                   `json:"profile_gate_reason,omitempty"`
	ProfileGateSummary         string                   `json:"profile_gate_summary,omitempty"`
	ProfileGateBlockedWork     bool                     `json:"profile_gate_blocked_work,omitempty"`
	Trigger                    string                   `json:"trigger,omitempty"`
	ClaimAction                string                   `json:"claim_action,omitempty"`
	SessionAction              string                   `json:"session_action,omitempty"`
	ResumeSummary              string                   `json:"resume_summary,omitempty"`
	ProjectID                  string                   `json:"project_id,omitempty"`
	TaskKind                   string                   `json:"task_kind,omitempty"`
	ProjectLane                string                   `json:"project_lane,omitempty"`
	RequiresProjectGate        bool                     `json:"requires_project_gate,omitempty"`
	ProjectCoordination        json.RawMessage          `json:"project_coordination,omitempty"`
	Packet                     *AgentWorkPacket         `json:"packet,omitempty"`
	Task                       *WorkspaceTaskRecord     `json:"task,omitempty"`
	Session                    *AgentSessionStateRecord `json:"session,omitempty"`
	Hydration                  *TaskHydrationBundle     `json:"hydration,omitempty"`
}

type AgentWorkPacket struct {
	WorkType            string                        `json:"work_type"`
	ProjectID           string                        `json:"project_id,omitempty"`
	TaskKind            string                        `json:"task_kind,omitempty"`
	ProjectLane         string                        `json:"project_lane,omitempty"`
	RequiresProjectGate bool                          `json:"requires_project_gate,omitempty"`
	ProjectCoordination json.RawMessage               `json:"project_coordination,omitempty"`
	CoordinationState   string                        `json:"coordination_state,omitempty"`
	PreferredTransition string                        `json:"preferred_transition,omitempty"`
	WhyNow              string                        `json:"why_now,omitempty"`
	Resume              *AgentWorkResumePacket        `json:"resume,omitempty"`
	Decision            *AgentWorkDecisionPacket      `json:"decision,omitempty"`
	Gate                *AgentWorkGatePacket          `json:"gate,omitempty"`
	Unblock             *AgentWorkUnblockPacket       `json:"unblock,omitempty"`
	Handoff             *AgentWorkHandoffPacket       `json:"handoff,omitempty"`
	Blockers            []model.AgentUpdateBlockedRef `json:"blockers,omitempty"`
	HandoffToAgentID    string                        `json:"handoff_to_agent_id,omitempty"`
	ContextHints        AgentWorkContextHints         `json:"context_hints,omitempty"`
	OwnerBound          *AgentWorkOwnerBoundPacket    `json:"owner_bound,omitempty"`
	PatchQueueSupersede *AgentWorkPatchQueueSupersede `json:"patch_queue_supersede,omitempty"`
	PatchQueueClaim     *AgentWorkPatchQueueClaim     `json:"patch_queue_claim_stewardship,omitempty"`
	Frontier            *AgentWorkTaskFrontier        `json:"frontier,omitempty"`
	Advisory            *AgentWorkAdvisory            `json:"advisory,omitempty"`
}

type AgentWorkTaskFrontier struct {
	GenerationID  string                           `json:"generation_id"`
	GeneratedAt   string                           `json:"generated_at"`
	SelectionMode string                           `json:"selection_mode"`
	Summary       string                           `json:"summary,omitempty"`
	Candidates    []AgentWorkTaskFrontierCandidate `json:"candidates,omitempty"`
	Roster        []AgentWorkRosterAgent           `json:"roster,omitempty"`
}

type AgentWorkTaskFrontierCandidate struct {
	Task           WorkspaceTaskRecord `json:"task"`
	Fit            AgentWorkTaskFit    `json:"fit"`
	ClaimAction    string              `json:"claim_action,omitempty"`
	SessionAction  string              `json:"session_action,omitempty"`
	Blocked        bool                `json:"blocked,omitempty"`
	BlockReason    string              `json:"block_reason,omitempty"`
	BlockSummary   string              `json:"block_summary,omitempty"`
	AdvisoryReason string              `json:"advisory_reason,omitempty"`
}

type AgentWorkTaskFit struct {
	Level              string   `json:"level"`
	Score              int      `json:"score"`
	Reasons            []string `json:"reasons,omitempty"`
	RequiredWorkModes  []string `json:"required_work_modes,omitempty"`
	PreferredWorkModes []string `json:"preferred_work_modes,omitempty"`
	PreferredSkills    []string `json:"preferred_skills,omitempty"`
	PreferredTools     []string `json:"preferred_tools,omitempty"`
	AdvisoryRoleTypes  []string `json:"advisory_role_types,omitempty"`
}

type AgentWorkRosterAgent struct {
	AgentID               string             `json:"agent_id"`
	DisplayName           string             `json:"display_name,omitempty"`
	Role                  string             `json:"role,omitempty"`
	Status                string             `json:"status,omitempty"`
	IsOnline              bool               `json:"is_online"`
	LastSeenAt            *string            `json:"last_seen_at,omitempty"`
	ActiveTaskCount       int                `json:"active_task_count"`
	CurrentSessionID      string             `json:"current_session_id,omitempty"`
	CurrentTaskIDs        []string           `json:"current_task_ids,omitempty"`
	Capabilities          []string           `json:"capabilities,omitempty"`
	ProfileSpecialization string             `json:"profile_specialization,omitempty"`
	ProfileTags           []string           `json:"profile_tags,omitempty"`
	ToolsAccess           []string           `json:"tools_access,omitempty"`
	Busyness              string             `json:"busyness,omitempty"`
	ActiveTasks           []AgentCurrentTask `json:"active_tasks,omitempty"`
}

type AgentWorkResumePacket struct {
	SessionID string `json:"session_id"`
	Summary   string `json:"summary,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type AgentWorkDecisionPacket struct {
	NeededFrom   string `json:"needed_from,omitempty"`
	DecisionType string `json:"decision_type,omitempty"`
}

type AgentWorkGatePacket struct {
	GateState  string `json:"gate_state,omitempty"`
	GateType   string `json:"gate_type,omitempty"`
	NeededFrom string `json:"needed_from,omitempty"`
	Summary    string `json:"summary,omitempty"`
}

type AgentWorkUnblockPacket struct {
	UnblockState string   `json:"unblock_state,omitempty"`
	Trigger      string   `json:"trigger,omitempty"`
	BlockerKinds []string `json:"blocker_kinds,omitempty"`
	Summary      string   `json:"summary,omitempty"`
}

type AgentWorkHandoffPacket struct {
	HandoffState string `json:"handoff_state,omitempty"`
	ToAgentID    string `json:"to_agent_id,omitempty"`
	Summary      string `json:"summary,omitempty"`
}

type AgentWorkContextHints struct {
	SuggestedDocKeys      []string `json:"suggested_doc_keys,omitempty"`
	RelatedArtifactRefs   []string `json:"related_artifact_refs,omitempty"`
	AnchorTaskIDs         []string `json:"anchor_task_ids,omitempty"`
	AnchorConflictTaskIDs []string `json:"anchor_conflict_task_ids,omitempty"`
	AnchorBranchIDs       []string `json:"anchor_branch_ids,omitempty"`
	AnchorSessionIDs      []string `json:"anchor_session_ids,omitempty"`
}

type AgentWorkOwnerBoundPacket struct {
	Kind            string `json:"kind,omitempty"`
	RequiredAgentID string `json:"required_agent_id,omitempty"`
	BranchID        string `json:"branch_id,omitempty"`
	BranchName      string `json:"branch_name,omitempty"`
	HeadSHA         string `json:"head_sha,omitempty"`
	ReviewDocKey    string `json:"review_doc_key,omitempty"`
	QueueID         string `json:"queue_id,omitempty"`
	ItemID          string `json:"item_id,omitempty"`
	RepairNeeded    bool   `json:"repair_needed,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

type AgentWorkPatchQueueSupersede struct {
	ProjectID      string `json:"project_id,omitempty"`
	QueueID        string `json:"queue_id,omitempty"`
	ItemID         string `json:"item_id,omitempty"`
	BranchID       string `json:"branch_id,omitempty"`
	BranchName     string `json:"branch_name,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	NewItemID      string `json:"new_item_id,omitempty"`
	EvidenceDocKey string `json:"evidence_doc_key,omitempty"`
	DecisionDocKey string `json:"decision_doc_key,omitempty"`
	ReviewDocKey   string `json:"review_doc_key,omitempty"`
	Summary        string `json:"summary,omitempty"`
}

type AgentWorkPatchQueueClaim struct {
	ProjectID               string   `json:"project_id,omitempty"`
	QueueID                 string   `json:"queue_id,omitempty"`
	ItemID                  string   `json:"item_id,omitempty"`
	BranchID                string   `json:"branch_id,omitempty"`
	BranchName              string   `json:"branch_name,omitempty"`
	HeadSHA                 string   `json:"head_sha,omitempty"`
	State                   string   `json:"state,omitempty"`
	ClaimedBy               string   `json:"claimed_by,omitempty"`
	ClaimExpiresAt          string   `json:"claim_expires_at,omitempty"`
	ClaimActive             bool     `json:"claim_active,omitempty"`
	OperationBindingPresent bool     `json:"operation_binding_present,omitempty"`
	ReviewDocKey            string   `json:"review_doc_key,omitempty"`
	EvidenceDocKey          string   `json:"evidence_doc_key,omitempty"`
	DecisionDocKey          string   `json:"decision_doc_key,omitempty"`
	AllowedActions          []string `json:"allowed_actions,omitempty"`
	Summary                 string   `json:"summary,omitempty"`
}

type AgentWorkAdvisory struct {
	ProtoClusterID string                     `json:"proto_cluster_id,omitempty"`
	Control        *AgentWorkControlAdvisory  `json:"control,omitempty"`
	Corridor       *AgentWorkCorridorAdvisory `json:"corridor,omitempty"`
	Frontier       []TensionFrontierItem      `json:"frontier,omitempty"`
}

type AgentWorkControlAdvisory struct {
	AttentionBand string `json:"attention_band,omitempty"`
	PressureScore int    `json:"pressure_score,omitempty"`
	Summary       string `json:"summary,omitempty"`
	BasisStale    bool   `json:"basis_stale,omitempty"`
}

type AgentWorkCorridorAdvisory struct {
	CorridorReadiness   string `json:"corridor_readiness,omitempty"`
	TaskClassHint       string `json:"task_class_hint,omitempty"`
	CorridorCatalogHint string `json:"corridor_catalog_hint,omitempty"`
	Summary             string `json:"summary,omitempty"`
	BasisStale          bool   `json:"basis_stale,omitempty"`
}

func (s *Store) GetAgentWorkNext(ctx context.Context, filter AgentWorkNextFilter) (AgentWorkNextResult, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return AgentWorkNextResult{}, ErrWorkspaceNotFound
	}
	agentID := strings.TrimSpace(filter.AgentID)
	if agentID == "" {
		return AgentWorkNextResult{}, ErrAgentNotFound
	}

	if _, err := s.GetWorkspace(ctx, workspaceID); err != nil {
		return AgentWorkNextResult{}, err
	}
	agent, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	profile, err := s.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	allowAutonomousExecution, profileGateReason, profileGateSummary := agentProfileAutonomousExecutionGate(profile)
	trustFirst := coordinationModeTrustFirst(filter.CoordinationMode)
	if trustFirst {
		allowAutonomousExecution = true
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	generatedAt := generatedAtFromWorkspaceTimeAuthority(authority)

	grp, err := s.GetAgentLimitGroup(ctx, workspaceID, agentID)
	if err != nil {
		return AgentWorkNextResult{}, fmt.Errorf("check agent limits: %w", err)
	}
	if grp != nil && grp.DailyRemaining <= 0 && !trustFirst {
		return AgentWorkNextResult{
			GeneratedAt:                generatedAt,
			TimeAuthority:              authority,
			WorkspaceID:                workspaceID,
			AgentID:                    agentID,
			HasWork:                    false,
			Reason:                     "budget_exceeded",
			AutonomousExecutionAllowed: allowAutonomousExecution,
			ProfileGateReason:          profileGateReason,
			ProfileGateSummary:         profileGateSummary,
		}, nil
	}

	result := AgentWorkNextResult{
		GeneratedAt:                generatedAt,
		TimeAuthority:              authority,
		WorkspaceID:                workspaceID,
		AgentID:                    agentID,
		Reason:                     "none_available",
		AutonomousExecutionAllowed: allowAutonomousExecution,
		ProfileGateReason:          profileGateReason,
		ProfileGateSummary:         profileGateSummary,
	}

	exclusions, err := s.GetAgentTensionExclusions(ctx, workspaceID, agentID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	excludedMap := make(map[string]struct{}, len(exclusions))
	for _, id := range exclusions {
		excludedMap[id] = struct{}{}
	}

	tasksRaw, err := s.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	// P1 + P1.5 (work.next selection liveness, A1 universal): the pre-selection reconcilers are
	// best-effort LIVENESS sweeps (release expired claims / re-attempt DEFERRED continuations /
	// terminalize stale-receipt carriers), NOT this agent's selection correctness. Each runs in its
	// OWN bounded sub-context so a slow sweep can never consume the shared RPC deadline and poison
	// downstream candidate selection - the c2 root, where a context-deadline-exceeded inside the
	// receipt sweep returned wholesale and aborted work.next roster-wide. Any sweep-level error
	// (incl. DeadlineExceeded) is swallowed + journaled so selection ALWAYS proceeds; the next poll
	// retries. Swallowing is safe: the sweeps mutate only cleanup state and per-candidate guards
	// downstream re-validate, so a freshly-materialized owner-bound carrier still reaches selection
	// (the integration carrier is materialized by the DEFERRED-continuation sweep, committed before
	// selection, then surfaced via next_pending).
	func() {
		recCtx, cancel := context.WithTimeout(ctx, agentWorkPreSelectionSweepBudget)
		defer cancel()
		// RPF-58C: release expired patch-queue claims back to PROPOSED so a stale CLAIMED item
		// (claimed-then-wandered reviewer) cannot wedge the queue or the steward lane.
		if err := s.reconcileExpiredProjectPatchQueueClaims(recCtx, workspaceID); err != nil {
			s.journalAgentWorkPreSelectionSweepFailure(ctx, workspaceID, "reconcile_expired_patch_queue_claims", err)
		}
	}()
	func() {
		recCtx, cancel := context.WithTimeout(ctx, agentWorkPreSelectionSweepBudget)
		defer cancel()
		// Stage-4 decisive-path liveness (I2): re-attempt DEFERRED decision continuations; a
		// continuation whose awaited role now exists materializes into a claimable carrier in the
		// SAME cycle. Journals its own sweep-level failure internally; swallow here too.
		_ = s.ReconcileDeferredProjectPatchQueueContinuations(recCtx, workspaceID)
	}()
	var receiptTerminalizedTaskIDs map[string]struct{}
	func() {
		recCtx, cancel := context.WithTimeout(ctx, agentWorkPreSelectionSweepBudget)
		defer cancel()
		var ids map[string]struct{}
		var rerr error
		if agentWorkRequiredReceiptSweepFault != nil {
			rerr = agentWorkRequiredReceiptSweepFault() // test-only fault injection (P5); nil in production
		} else {
			ids, rerr = s.reconcileAgentWorkRequiredReceiptTerminals(recCtx, workspaceID, tasksRaw)
		}
		if rerr != nil {
			s.journalAgentWorkPreSelectionSweepFailure(ctx, workspaceID, "reconcile_required_receipt_terminals", rerr)
			return
		}
		receiptTerminalizedTaskIDs = ids
	}()
	if len(receiptTerminalizedTaskIDs) > 0 {
		tasksRaw, err = s.ListWorkspaceTasks(ctx, workspaceID)
		if err != nil {
			return AgentWorkNextResult{}, err
		}
	}

	tasks := make([]WorkspaceTaskRecord, 0, len(tasksRaw))
	for _, t := range tasksRaw {
		if _, ok := excludedMap[t.TaskID]; !ok {
			tasks = append(tasks, t)
		}
	}

	sessionLimit := filter.SessionLimit
	if sessionLimit <= 0 {
		sessionLimit = 100
	}
	sessions, err := s.ListWorkspaceSessionStates(ctx, workspaceID, true, sessionLimit)
	if err != nil {
		return AgentWorkNextResult{}, err
	}

	taskIndex := make(map[string]WorkspaceTaskRecord, len(tasks))
	for _, task := range tasks {
		taskIndex[task.TaskID] = task
	}
	taskDependencyBlocks, err := s.unresolvedWorkspaceTaskDependencyMap(ctx, workspaceID)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	taskDependencyBlocks, err = s.filterSupersededAgentWorkDependencyBlocks(ctx, workspaceID, taskIndex, taskDependencyBlocks)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	productLanePressureProjects, err := s.agentWorkProductLanePressureProjects(ctx, workspaceID, tasks)
	if err != nil {
		return AgentWorkNextResult{}, err
	}
	taskProjectByID := agentWorkTaskProjectByID(tasks)

	trigger := normalizeAgentWorkTrigger(filter.Trigger)
	candidateTaskID := strings.TrimSpace(filter.CandidateTaskID)
	candidateSessionID := strings.TrimSpace(filter.CandidateSessionID)
	if trigger != "" && candidateTaskID != "" {
		if candidateTask, ok := taskIndex[candidateTaskID]; ok && isTerminalTaskStatus(candidateTask.Status) {
			terminalizedByReceiptSweep := false
			if _, ok := receiptTerminalizedTaskIDs[candidateTaskID]; ok {
				terminalizedByReceiptSweep = true
			} else {
				var err error
				terminalizedByReceiptSweep, err = s.agentWorkTaskHasRequiredReceiptTerminalization(ctx, candidateTaskID)
				if err != nil {
					return AgentWorkNextResult{}, err
				}
			}
			if terminalizedByReceiptSweep {
				// Contract: a receipt-swept trigger candidate returns the superseded
				// packet so the runtime clears the dead trigger; the agent gets fresh
				// work on its NEXT poll (live-proven in R42: superseded -> repoll ->
				// fresh product task 12s later). The no-starvation invariant across the
				// poll boundary is locked by the follow-up-poll assertion in
				// TestReadyBranchPatchQueueSubmitHandoffSuppressesWhenItemOrTaskExists.
				result.Reason = "trigger_task_superseded"
				result.Trigger = trigger
				attachAgentWorkResultTaskProjectDigest(&result, candidateTask)
				if filter.IncludePacket {
					result.Packet = taskSupersededPacket(candidateTask)
					s.attachProjectCoordinationToAgentWork(ctx, &result, &candidateTask, result.Packet)
				}
				return result, nil
			}
		}
	}
	if triggeredTask, triggeredSession, ok := selectTriggeredAgentWork(taskIndex, sessions, agentID, trigger, candidateTaskID, candidateSessionID); ok {
		if activeTask, activeSession, activeSummary, rerouted := selectTriggeredActiveLanePublicationResume(taskIndex, sessions, agentID, trigger, *triggeredTask); rerouted {
			triggeredTask = activeTask
			triggeredSession = activeSession
			result.ResumeSummary = activeSummary
		}
		if _, terminalizedByReceiptSweep := receiptTerminalizedTaskIDs[strings.TrimSpace(triggeredTask.TaskID)]; terminalizedByReceiptSweep {
			result.Reason = "trigger_task_superseded"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = taskSupersededPacket(*triggeredTask)
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if superseded {
			result.Reason = "trigger_task_superseded"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = taskSupersededPacket(*triggeredTask)
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, triggeredTask.TaskID); len(blockers) > 0 {
			result.Reason = "task_dependency_blocked"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = taskDependencyBlockedPacket(*triggeredTask, blockers)
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if triggeredSession == nil && agentWorkTaskBlockedByProductLanePressure(*triggeredTask, productLanePressureProjects, taskProjectByID) {
			result.Reason = "product_lane_pressure"
			result.Trigger = trigger
			result.ResumeSummary = productLanePressureCoordinationBlockSummary()
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				s.attachAgentWorkPacket(ctx, &result, filter)
			}
			return result, nil
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			if recoveryTask, ok, err := s.projectStrategicLeadRecoveryTaskForGate(ctx, workspaceID, agentID, tasks, taskDependencyBlocks, nil, *triggeredTask, packet); err != nil {
				return AgentWorkNextResult{}, err
			} else if ok {
				result.HasWork = true
				result.Reason = "project_strategic_lead_recovery"
				taskCopy := *recoveryTask
				result.Task = &taskCopy
				result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
				result.SessionAction = "start_new"
				result.ResumeSummary = "Recover active strategic lead lease before resuming implementation-gated project work."
				if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
					return AgentWorkNextResult{}, err
				}
				s.attachAgentWorkPacket(ctx, &result, filter)
				return result, nil
			}
			result.Reason = "project_gate_closed"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
		if triggeredSession == nil {
			if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, *triggeredTask, trustFirst); err != nil {
				return AgentWorkNextResult{}, err
			} else if blocked {
				result.Reason = firstNonEmpty(strings.TrimSpace(packet.WorkType), "project_claim_admission_blocked")
				result.Trigger = trigger
				attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
				if filter.IncludePacket {
					result.Packet = packet
					s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
				}
				return result, nil
			}
		}
		if agentWorkTaskIsProactiveMetacognition(*triggeredTask) && !agentProfileAllowsProactiveMetacognitionTask(profile, *triggeredTask) {
			result.Reason = "trigger_no_work"
			result.Trigger = trigger
			result.ProfileGateBlockedWork = true
			result.ProfileGateReason = "metacognition_scope_mismatch"
			result.ProfileGateSummary = fmt.Sprintf("Agent reflection_scope=%s is not eligible for triggered metacognition task scope=%s.", agentProfileReflectionScope(profile), firstNonEmpty(agentWorkTaskMetacognitionScope(*triggeredTask), "unspecified"))
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				s.attachAgentWorkPacket(ctx, &result, filter)
			}
			return result, nil
		}
		if triggeredSession == nil && !agentWorkPatchQueueReviewTask(*triggeredTask) && !agentProfileAllowsFreshTaskSelectionForMode(profile, *triggeredTask, trustFirst) {
			bypass, err := s.agentWorkMayBypassFreshProfileGate(ctx, workspaceID, agentID, *triggeredTask)
			if err != nil {
				return AgentWorkNextResult{}, err
			}
			if !bypass {
				result.Reason = "trigger_no_work"
				result.Trigger = trigger
				result.ProfileGateBlockedWork = true
				result.ProfileGateReason = "profile_task_mode_mismatch"
				result.ProfileGateSummary = fmt.Sprintf("Agent fresh-selection mode %s is not eligible for triggered task %s.", agentProfileFreshSelectionMode(profile), strings.TrimSpace(triggeredTask.TaskID))
				attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
				if filter.IncludePacket {
					s.attachAgentWorkPacket(ctx, &result, filter)
				}
				return result, nil
			}
		}
		if ok, err := s.agentMaySelectProjectClaimRepairTask(ctx, workspaceID, agentID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			result.Reason = "project_claim_repair_lead_required"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = projectStrategicLeadCoordinationRequiredPacket(*triggeredTask)
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if !agentWorkABPCRecoveryActionBypassesProjectRoleLane(*triggeredTask) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, *triggeredTask) {
			if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, *triggeredTask); err != nil {
				return AgentWorkNextResult{}, err
			} else if !ok {
				result.Reason = "project_role_lane_required"
				result.Trigger = trigger
				attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
				if filter.IncludePacket {
					result.Packet = projectRoleLaneRequiredPacket(*triggeredTask)
					s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
				}
				return result, nil
			}
		}
		if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			if agentWorkPatchQueueReviewReceiptTask(*triggeredTask) {
				result.Reason = "trigger_no_work"
				result.Trigger = trigger
				attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
				return result, nil
			}
			result.Reason = "project_patch_queue_review_role_required"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = projectPatchQueueReviewRoleRequiredPacket(*triggeredTask)
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			if recoveryTask, ok, err := s.projectStrategicLeadRecoveryTaskForGate(ctx, workspaceID, agentID, tasks, taskDependencyBlocks, nil, *triggeredTask, packet); err != nil {
				return AgentWorkNextResult{}, err
			} else if ok {
				result.HasWork = true
				result.Reason = "project_strategic_lead_recovery"
				taskCopy := *recoveryTask
				result.Task = &taskCopy
				result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
				result.SessionAction = "start_new"
				result.ResumeSummary = "Recover active strategic lead lease before resuming implementation-gated project work."
				if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
					return AgentWorkNextResult{}, err
				}
				s.attachAgentWorkPacket(ctx, &result, filter)
				return result, nil
			}
			result.Reason = "project_gate_closed"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, *triggeredTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			result.Reason = "project_validation_artifact_missing"
			result.Trigger = trigger
			attachAgentWorkResultTaskProjectDigest(&result, *triggeredTask)
			if filter.IncludePacket {
				result.Packet = packet
				s.attachProjectCoordinationToAgentWork(ctx, &result, triggeredTask, result.Packet)
			}
			return result, nil
		}
		if !projectStrategicLeadCoordinationTask(*triggeredTask) {
			if authorityTask, ok, err := s.selectAgentWorkStrategicLeadAuthorityTransition(ctx, workspaceID, agentID, *triggeredTask, tasks, taskDependencyBlocks, nil, nil); err != nil {
				return AgentWorkNextResult{}, err
			} else if ok {
				taskCopy := *authorityTask
				result.HasWork = true
				result.Reason = projectStrategicLeadAuthorityTransitionReason(taskCopy)
				result.Trigger = trigger
				result.Task = &taskCopy
				result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
				result.SessionAction = "start_new"
				result.ResumeSummary = "Strategic lead repair transition must be applied durably before the triggered project lane can continue."
				if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
					return AgentWorkNextResult{}, err
				}
				s.attachAgentWorkPacket(ctx, &result, filter)
				return result, nil
			}
		}
		result.HasWork = true
		result.Reason = reasonForAgentWorkSelection(triggeredSession)
		if projectStrategicLeadCoordinationTask(*triggeredTask) {
			result.Reason = projectStrategicLeadAuthorityTransitionReason(*triggeredTask)
		}
		result.Trigger = trigger
		result.Task = triggeredTask
		result.Session = triggeredSession
		result.ClaimAction = claimActionForAgentWork(*triggeredTask, agentID)
		result.SessionAction = sessionActionForAgentWork(triggeredSession, trigger)
		if result.ResumeSummary == "" {
			result.ResumeSummary = resumeSummaryForAgentWork(triggeredSession, trigger)
		}
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}
	if isAgentWorkTaskSwitchTrigger(trigger) && candidateTaskID != "" {
		result.Reason = triggeredAgentWorkNoWorkReason(taskIndex, sessions, agentID, trigger, candidateTaskID, candidateSessionID)
		result.Trigger = trigger
		if task, ok := taskIndex[candidateTaskID]; ok {
			attachAgentWorkResultTaskProjectDigest(&result, task)
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}
	if trigger == "system_news" && (candidateTaskID != "" || candidateSessionID != "") {
		result.Reason = "trigger_no_work"
		result.Trigger = trigger
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}

	var selectedTask *WorkspaceTaskRecord
	var selectedSession *AgentSessionStateRecord
	agentSessionTasks := make(map[string]struct{}, len(sessions))
	pausedTasks := make(map[string]struct{}, len(sessions))
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != agentID || strings.TrimSpace(session.TaskID) == "" {
			continue
		}
		if isEndedAgentWorkSessionStatus(session.Status) {
			continue
		}
		agentSessionTasks[session.TaskID] = struct{}{}
		if !isRunnableAgentWorkSessionStatus(session.Status) {
			pausedTasks[session.TaskID] = struct{}{}
			continue
		}
		task, ok := taskIndex[session.TaskID]
		if !ok || isTerminalTaskStatus(task.Status) {
			continue
		}
		if !isResumableClaimForAgent(task, agentID) {
			continue
		}
		taskCopy := task
		sessionCopy := session
		selectedTask = &taskCopy
		selectedSession = &sessionCopy
		break
	}
	var firstProjectGateBlockedTask *WorkspaceTaskRecord
	var firstProjectGateBlockedPacket *AgentWorkPacket
	var firstProjectValidationArtifactBlockedTask *WorkspaceTaskRecord
	var firstProjectValidationArtifactBlockedPacket *AgentWorkPacket
	var firstTaskDependencyBlockedTask *WorkspaceTaskRecord
	var firstTaskDependencyBlockedPacket *AgentWorkPacket
	var firstProjectClaimScopeBusyTask *WorkspaceTaskRecord
	var firstProjectClaimScopeBusyPacket *AgentWorkPacket
	var firstProjectClaimRepairLeadRequiredTask *WorkspaceTaskRecord
	var firstProjectClaimRepairLeadRequiredPacket *AgentWorkPacket
	var firstProjectClaimRepairBackstopTask *WorkspaceTaskRecord
	var firstProjectClaimAdmissionUnclaimableTask *WorkspaceTaskRecord
	var firstProjectClaimAdmissionUnclaimablePacket *AgentWorkPacket
	var firstProjectRoleLaneRequiredTask *WorkspaceTaskRecord
	var firstProjectRoleLaneRequiredPacket *AgentWorkPacket
	var firstProjectPatchQueueReviewRoleRequiredTask *WorkspaceTaskRecord
	var firstProjectPatchQueueReviewRoleRequiredPacket *AgentWorkPacket
	var firstProjectOwnerBoundTask *WorkspaceTaskRecord
	var firstProjectOwnerBoundPacket *AgentWorkPacket
	var firstProjectTargetedDelegationTask *WorkspaceTaskRecord
	var firstProjectTargetedDelegationPacket *AgentWorkPacket
	rememberProjectGateBlockedTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectGateBlockedTask != nil {
			return
		}
		taskCopy := task
		firstProjectGateBlockedTask = &taskCopy
		firstProjectGateBlockedPacket = packet
	}
	rememberProjectValidationArtifactBlockedTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectValidationArtifactBlockedTask != nil {
			return
		}
		taskCopy := task
		firstProjectValidationArtifactBlockedTask = &taskCopy
		firstProjectValidationArtifactBlockedPacket = packet
	}
	rememberTaskDependencyBlockedTask := func(task WorkspaceTaskRecord, blockers []string) {
		if firstTaskDependencyBlockedTask != nil {
			return
		}
		taskCopy := task
		firstTaskDependencyBlockedTask = &taskCopy
		firstTaskDependencyBlockedPacket = taskDependencyBlockedPacket(task, blockers)
	}
	rememberProjectClaimScopeBusyTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectClaimScopeBusyTask != nil {
			return
		}
		taskCopy := task
		firstProjectClaimScopeBusyTask = &taskCopy
		firstProjectClaimScopeBusyPacket = packet
	}
	rememberProjectClaimRepairLeadRequiredTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectClaimRepairLeadRequiredTask != nil {
			return
		}
		taskCopy := task
		firstProjectClaimRepairLeadRequiredTask = &taskCopy
		firstProjectClaimRepairLeadRequiredPacket = packet
	}
	rememberProjectClaimRepairBackstopTask := func(task WorkspaceTaskRecord) {
		if firstProjectClaimRepairBackstopTask != nil {
			return
		}
		taskCopy := task
		firstProjectClaimRepairBackstopTask = &taskCopy
	}
	rememberProjectClaimAdmissionUnclaimableTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectClaimAdmissionUnclaimableTask != nil {
			return
		}
		taskCopy := task
		firstProjectClaimAdmissionUnclaimableTask = &taskCopy
		firstProjectClaimAdmissionUnclaimablePacket = packet
	}
	rememberProjectRoleLaneRequiredTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectRoleLaneRequiredTask != nil {
			return
		}
		taskCopy := task
		firstProjectRoleLaneRequiredTask = &taskCopy
		firstProjectRoleLaneRequiredPacket = packet
	}
	rememberProjectPatchQueueReviewRoleRequiredTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectPatchQueueReviewRoleRequiredTask != nil {
			return
		}
		taskCopy := task
		firstProjectPatchQueueReviewRoleRequiredTask = &taskCopy
		firstProjectPatchQueueReviewRoleRequiredPacket = packet
	}
	rememberProjectOwnerBoundTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectOwnerBoundTask != nil {
			return
		}
		taskCopy := task
		firstProjectOwnerBoundTask = &taskCopy
		firstProjectOwnerBoundPacket = packet
	}
	rememberProjectTargetedDelegationTask := func(task WorkspaceTaskRecord, packet *AgentWorkPacket) {
		if firstProjectTargetedDelegationTask != nil {
			return
		}
		taskCopy := task
		firstProjectTargetedDelegationTask = &taskCopy
		firstProjectTargetedDelegationPacket = packet
	}
	tryPatchQueueSubmitHandoffForTask := func(task WorkspaceTaskRecord) (AgentWorkNextResult, bool, error) {
		if !agentWorkTaskAllowsPatchQueueSubmitHandoffPreemption(task) {
			return AgentWorkNextResult{}, false, nil
		}
		packet, ok, err := s.agentWorkPatchQueueSubmitHandoffAvailable(ctx, workspaceID, agentID, tasks, taskDependencyBlocks)
		if err != nil || !ok {
			return AgentWorkNextResult{}, ok, err
		}
		handoffResult := result
		handoffResult.Reason = "project_patch_queue_submit_handoff_available"
		handoffResult.ProjectID = strings.TrimSpace(packet.ProjectID)
		handoffResult.TaskKind = strings.TrimSpace(packet.TaskKind)
		handoffResult.ProjectLane = strings.TrimSpace(packet.ProjectLane)
		handoffResult.RequiresProjectGate = false
		if filter.IncludePacket {
			handoffResult.Packet = packet
		}
		return handoffResult, true, nil
	}
	var busyTasks map[string]struct{}
	loadBusyTasks := func() (map[string]struct{}, error) {
		if busyTasks != nil {
			return busyTasks, nil
		}
		busyTasks = make(map[string]struct{}, len(sessions))
		for _, session := range sessions {
			taskID := strings.TrimSpace(session.TaskID)
			if taskID == "" || strings.TrimSpace(session.AgentID) == agentID {
				continue
			}
			if isEndedAgentWorkSessionStatus(session.Status) {
				continue
			}
			ok, err := executionSessionHasStartReceipt(ctx, s.db, workspaceID, strings.TrimSpace(session.SessionID), strings.TrimSpace(session.AgentID), taskID)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			busyTasks[taskID] = struct{}{}
		}
		return busyTasks, nil
	}
	returnPatchQueueReviewTask := func(task WorkspaceTaskRecord, reason string) (AgentWorkNextResult, error) {
		taskCopy := task
		reviewResult := result
		reviewResult.HasWork = true
		reviewResult.Reason = reason
		reviewResult.Task = &taskCopy
		reviewResult.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
		reviewResult.SessionAction = "start_new"
		reviewResult.ResumeSummary = "A PROPOSED patch queue item has a durable review receipt; reviewer/integrator work preempts stale sidecar or coordination loops."
		if err := s.attachAgentWorkHydration(ctx, &reviewResult, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &reviewResult, filter)
		return reviewResult, nil
	}

	if selectedTask != nil {
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if superseded {
			selectedTask = nil
			selectedSession = nil
		}
	}
	if selectedTask != nil && selectedSession == nil && agentWorkTaskBlockedByProductLanePressure(*selectedTask, productLanePressureProjects, taskProjectByID) {
		selectedTask = nil
		selectedSession = nil
	}

	if selectedTask != nil {
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, selectedTask.TaskID); len(blockers) > 0 {
			rememberTaskDependencyBlockedTask(*selectedTask, blockers)
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(*selectedTask, packet)
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if ok, err := s.agentMaySelectProjectClaimRepairTask(ctx, workspaceID, agentID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			rememberProjectClaimRepairLeadRequiredTask(*selectedTask, projectStrategicLeadCoordinationRequiredPacket(*selectedTask))
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, *selectedTask, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_scope_busy") {
				rememberProjectClaimScopeBusyTask(*selectedTask, packet)
			} else if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_admission_unclaimable") {
				rememberProjectClaimAdmissionUnclaimableTask(*selectedTask, packet)
			} else {
				rememberProjectOwnerBoundTask(*selectedTask, packet)
			}
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil && !agentWorkABPCRecoveryActionBypassesProjectRoleLane(*selectedTask) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, *selectedTask) {
		if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			rememberProjectRoleLaneRequiredTask(*selectedTask, projectRoleLaneRequiredPacket(*selectedTask))
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			if !agentWorkPatchQueueReviewReceiptTask(*selectedTask) {
				rememberProjectPatchQueueReviewRoleRequiredTask(*selectedTask, projectPatchQueueReviewRoleRequiredPacket(*selectedTask))
			}
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(*selectedTask, packet)
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, *selectedTask); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectValidationArtifactBlockedTask(*selectedTask, packet)
			selectedTask = nil
			selectedSession = nil
		}
	}

	if selectedTask != nil {
		if authorityTask, ok, err := s.selectAgentWorkStrategicLeadAuthorityTransition(ctx, workspaceID, agentID, *selectedTask, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			taskCopy := *authorityTask
			result.HasWork = true
			result.Reason = projectStrategicLeadAuthorityTransitionReason(taskCopy)
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			result.ResumeSummary = "Strategic lead repair transition must be applied durably before the active project lane can continue."
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
		if recoveryTask, ok, err := s.selectAgentWorkABPCRecoverySuccessor(ctx, workspaceID, agentID, profile, *selectedTask, tasks, sessions, taskDependencyBlocks, agentSessionTasks, pausedTasks, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			taskCopy := *recoveryTask
			result.HasWork = true
			result.Reason = "abpc_recovery_successor_available"
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			result.ResumeSummary = "Side-effect classifier is waiting on a recovery successor; execute the successor instead of re-litigating the parent blocker."
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
		if allowAutonomousExecution && agentWorkTaskAllowsPatchQueueReviewPreemption(*selectedTask) {
			busy, err := loadBusyTasks()
			if err != nil {
				return AgentWorkNextResult{}, err
			}
			if reviewTask, ok, err := s.selectAgentWorkPendingPatchQueueReviewTask(ctx, workspaceID, agentID, profile, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks, busy, trustFirst); err != nil {
				return AgentWorkNextResult{}, err
			} else if ok {
				return returnPatchQueueReviewTask(*reviewTask, "project_patch_queue_review_available")
			}
		}
		result.HasWork = true
		result.Reason = "resume_session"
		result.Task = selectedTask
		result.Session = selectedSession
		result.ClaimAction = claimActionForAgentWork(*selectedTask, agentID)
		result.SessionAction = sessionActionForAgentWork(selectedSession, "")
		result.ResumeSummary = resumeSummaryForAgentWork(selectedSession, "")
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}

	if allowAutonomousExecution {
		if packet, ok, err := s.agentWorkPatchQueueSupersedeAvailable(ctx, workspaceID, agentID, tasks, taskDependencyBlocks); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.Reason = "project_patch_queue_supersede_available"
			result.ProjectID = strings.TrimSpace(packet.ProjectID)
			result.TaskKind = strings.TrimSpace(packet.TaskKind)
			result.ProjectLane = strings.TrimSpace(packet.ProjectLane)
			result.RequiresProjectGate = false
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
		if packet, ok, err := s.agentWorkPatchQueueClaimStewardshipAvailable(ctx, workspaceID, agentID, tasks, taskDependencyBlocks, authority); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.Reason = "project_patch_queue_claim_stewardship_available"
			result.ProjectID = strings.TrimSpace(packet.ProjectID)
			result.TaskKind = strings.TrimSpace(packet.TaskKind)
			result.ProjectLane = strings.TrimSpace(packet.ProjectLane)
			result.RequiresProjectGate = false
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
		if packet, ok, err := s.agentWorkPatchQueueMissingReviewTaskAvailable(ctx, workspaceID, agentID); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.Reason = "project_patch_queue_review_task_missing"
			result.ProjectID = strings.TrimSpace(packet.ProjectID)
			result.TaskKind = strings.TrimSpace(packet.TaskKind)
			result.ProjectLane = strings.TrimSpace(packet.ProjectLane)
			result.RequiresProjectGate = false
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
	}

	busyTasks, err = loadBusyTasks()
	if err != nil {
		return AgentWorkNextResult{}, err
	}

	if allowAutonomousExecution {
		if reviewTask, ok, err := s.selectAgentWorkPendingPatchQueueReviewTask(ctx, workspaceID, agentID, profile, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			return returnPatchQueueReviewTask(*reviewTask, "project_patch_queue_review_available")
		}
	}

	for _, task := range tasks {
		if !allowAutonomousExecution {
			continue
		}
		repairBackstopCandidate := false
		if projectStrategicLeadCoordinationTask(task) {
			if mode, err := s.agentProjectClaimRepairSelectionMode(ctx, workspaceID, agentID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if mode == projectClaimRepairSelectionDenied {
				continue
			} else if mode == projectClaimRepairSelectionBackstop {
				repairBackstopCandidate = true
			}
		} else {
			if !agentWorkPatchQueueReviewTask(task) && !agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst) {
				if isResumableClaimForAgent(task, agentID) && !agentWorkTaskIsPureStrategySelection(task) && !projectStrategicLeadCoordinationTask(task) {
					// Durable owned claims should resume before fresh frontier selection unless
					// the claim is a known stale project-root/strategy shape.
				} else {
					bypass, err := s.agentWorkMayBypassFreshProfileGate(ctx, workspaceID, agentID, task)
					if err != nil {
						return AgentWorkNextResult{}, err
					}
					if !bypass {
						continue
					}
				}
			}
		}
		if isTerminalTaskStatus(task.Status) || !isResumableClaimForAgent(task, agentID) {
			continue
		}
		if agentWorkTaskBlockedByProductLanePressure(task, productLanePressureProjects, taskProjectByID) {
			continue
		}
		if _, hasSession := agentSessionTasks[task.TaskID]; hasSession {
			continue
		}
		if _, paused := pausedTasks[task.TaskID]; paused {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			rememberTaskDependencyBlockedTask(task, blockers)
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(task, packet)
			continue
		}
		if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_scope_busy") {
				rememberProjectClaimScopeBusyTask(task, packet)
			} else if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_admission_unclaimable") {
				rememberProjectClaimAdmissionUnclaimableTask(task, packet)
			} else {
				rememberProjectOwnerBoundTask(task, packet)
			}
			continue
		}
		if !agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, task) {
			if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if !ok {
				rememberProjectRoleLaneRequiredTask(task, projectRoleLaneRequiredPacket(task))
				continue
			}
		}
		if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			if !agentWorkPatchQueueReviewReceiptTask(task) {
				rememberProjectPatchQueueReviewRoleRequiredTask(task, projectPatchQueueReviewRoleRequiredPacket(task))
			}
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(task, packet)
			continue
		}
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectValidationArtifactBlockedTask(task, packet)
			continue
		}
		if packet, targeted, err := s.projectImplementationFreshClaimRequiresTargetedSwitch(ctx, workspaceID, agentID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if targeted && !trustFirst {
			rememberProjectTargetedDelegationTask(task, packet)
			continue
		}
		if repairBackstopCandidate {
			rememberProjectClaimRepairBackstopTask(task)
			continue
		}
		if handoffResult, ok, err := tryPatchQueueSubmitHandoffForTask(task); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			return handoffResult, nil
		}
		taskCopy := task
		result.HasWork = true
		result.Reason = "resume_claim"
		result.Task = &taskCopy
		result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
		result.SessionAction = "start_new"
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}

	if allowAutonomousExecution && trigger == "" {
		if rootTask, ok, err := s.selectAgentWorkPendingOperatorRoot(ctx, workspaceID, agentID, profile, tasks, taskDependencyBlocks, busyTasks, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok && !agentWorkTaskBlockedByProductLanePressure(*rootTask, productLanePressureProjects, taskProjectByID) {
			if projectStrategicLeadCoordinationTask(*rootTask) {
				taskCopy := *rootTask
				result.HasWork = true
				result.Reason = projectStrategicLeadAuthorityTransitionReason(taskCopy)
				result.Task = &taskCopy
				result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
				result.SessionAction = "start_new"
				result.ResumeSummary = "Strategic lead repair transition must be applied durably before the project root lane resumes."
				if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
					return AgentWorkNextResult{}, err
				}
				s.attachAgentWorkPacket(ctx, &result, filter)
				return result, nil
			}
			if authorityTask, authorityOK, err := s.selectAgentWorkStrategicLeadAuthorityTransition(ctx, workspaceID, agentID, *rootTask, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks); err != nil {
				return AgentWorkNextResult{}, err
			} else if authorityOK {
				taskCopy := *authorityTask
				result.HasWork = true
				result.Reason = projectStrategicLeadAuthorityTransitionReason(taskCopy)
				result.Task = &taskCopy
				result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
				result.SessionAction = "start_new"
				result.ResumeSummary = "Strategic lead repair transition must be applied durably before the project root lane resumes."
				if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
					return AgentWorkNextResult{}, err
				}
				s.attachAgentWorkPacket(ctx, &result, filter)
				return result, nil
			}
			taskCopy := *rootTask
			result.HasWork = true
			result.Reason = "next_pending"
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
	}

	if allowAutonomousExecution {
		if continuationTask, ok, err := s.selectAgentWorkPatchQueueDecisionContinuationTask(ctx, workspaceID, agentID, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			taskCopy := *continuationTask
			result.HasWork = true
			result.Reason = "next_pending"
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
	}

	if allowAutonomousExecution {
		if pressureTask, ok, err := s.selectAgentWorkProductLanePressureTask(ctx, workspaceID, agentID, profile, tasks, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			taskCopy := *pressureTask
			if handoffResult, ok, err := tryPatchQueueSubmitHandoffForTask(taskCopy); err != nil {
				return AgentWorkNextResult{}, err
			} else if ok {
				return handoffResult, nil
			}
			result.HasWork = true
			result.Reason = "product_lane_pressure"
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
	}

	if allowAutonomousExecution && filter.EnableTaskFrontier && trigger == "" {
		frontier, err := s.buildAgentWorkTaskFrontier(ctx, workspaceID, agentID, generatedAt, profile, tasks, sessions, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, filter, trustFirst, productLanePressureProjects)
		if err != nil {
			return AgentWorkNextResult{}, err
		}
		if frontier != nil && len(frontier.Candidates) > 0 {
			result.HasWork = true
			result.Reason = "task_frontier_available"
			result.ClaimAction = "self_select"
			result.SessionAction = "self_select"
			result.Packet = &AgentWorkPacket{
				WorkType:            "task_frontier_available",
				CoordinationState:   "frontier_available",
				PreferredTransition: "self_select_task",
				WhyNow:              "agent requested autonomous task frontier",
				Frontier:            frontier,
			}
			return result, nil
		}
	}

	for _, task := range tasks {
		if !allowAutonomousExecution {
			continue
		}
		repairBackstopCandidate := false
		if projectStrategicLeadCoordinationTask(task) {
			if mode, err := s.agentProjectClaimRepairSelectionMode(ctx, workspaceID, agentID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if mode == projectClaimRepairSelectionDenied {
				continue
			} else if mode == projectClaimRepairSelectionBackstop {
				repairBackstopCandidate = true
			}
		} else {
			if !agentWorkPatchQueueReviewTask(task) && !agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst) {
				bypass, err := s.agentWorkMayBypassFreshProfileGate(ctx, workspaceID, agentID, task)
				if err != nil {
					return AgentWorkNextResult{}, err
				}
				if !bypass {
					continue
				}
			}
		}
		if task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
			continue
		}
		if agentWorkTaskBlockedByProductLanePressure(task, productLanePressureProjects, taskProjectByID) {
			continue
		}
		if _, busy := busyTasks[task.TaskID]; busy {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			rememberTaskDependencyBlockedTask(task, blockers)
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(task, packet)
			continue
		}
		if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_scope_busy") {
				rememberProjectClaimScopeBusyTask(task, packet)
			} else if strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_claim_admission_unclaimable") {
				rememberProjectClaimAdmissionUnclaimableTask(task, packet)
			} else {
				rememberProjectOwnerBoundTask(task, packet)
			}
			continue
		}
		if !agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, task) {
			if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if !ok {
				rememberProjectRoleLaneRequiredTask(task, projectRoleLaneRequiredPacket(task))
				continue
			}
		}
		if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if !ok {
			if !agentWorkPatchQueueReviewReceiptTask(task) {
				rememberProjectPatchQueueReviewRoleRequiredTask(task, projectPatchQueueReviewRoleRequiredPacket(task))
			}
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectGateBlockedTask(task, packet)
			continue
		}
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if blocked {
			rememberProjectValidationArtifactBlockedTask(task, packet)
			continue
		}
		if packet, targeted, err := s.projectImplementationFreshClaimRequiresTargetedSwitch(ctx, workspaceID, agentID, task); err != nil {
			return AgentWorkNextResult{}, err
		} else if targeted && !trustFirst {
			rememberProjectTargetedDelegationTask(task, packet)
			continue
		}
		if repairBackstopCandidate {
			rememberProjectClaimRepairBackstopTask(task)
			continue
		}
		if handoffResult, ok, err := tryPatchQueueSubmitHandoffForTask(task); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			return handoffResult, nil
		}
		taskCopy := task
		result.HasWork = true
		result.Reason = "next_pending"
		if projectStrategicLeadCoordinationTask(taskCopy) {
			result.Reason = projectStrategicLeadAuthorityTransitionReason(taskCopy)
		}
		result.Task = &taskCopy
		result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
		result.SessionAction = "start_new"
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}

	if allowAutonomousExecution {
		if packet, ok, err := s.agentWorkPatchQueueSubmitHandoffAvailable(ctx, workspaceID, agentID, tasks, taskDependencyBlocks); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.Reason = "project_patch_queue_submit_handoff_available"
			result.ProjectID = strings.TrimSpace(packet.ProjectID)
			result.TaskKind = strings.TrimSpace(packet.TaskKind)
			result.ProjectLane = strings.TrimSpace(packet.ProjectLane)
			result.RequiresProjectGate = false
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
	}

	if firstProjectOwnerBoundTask == nil {
		for _, task := range tasks {
			if isTerminalTaskStatus(task.Status) {
				continue
			}
			claimAgentID := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimAgentID))
			claimStatus := strings.ToUpper(strings.TrimSpace(workspaceTaskPointerValue(task.ClaimStatus)))
			if claimAgentID == "" || claimStatus != model.TaskClaimStatusClaimed {
				continue
			}
			if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if superseded {
				continue
			}
			if packet, blocked, err := s.projectOwnerBoundSelectionBlock(ctx, workspaceID, agentID, task); err != nil {
				return AgentWorkNextResult{}, err
			} else if blocked && packet != nil && packet.WorkType == "project_owner_bound_wrong_claim" {
				rememberProjectOwnerBoundTask(task, packet)
				break
			}
		}
	}

	if firstProjectClaimRepairBackstopTask != nil {
		result.HasWork = true
		result.Reason = "project_claim_repair_backstop"
		taskCopy := *firstProjectClaimRepairBackstopTask
		result.Task = &taskCopy
		result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
		result.SessionAction = "start_new"
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		s.attachAgentWorkPacket(ctx, &result, filter)
		return result, nil
	}

	if firstProjectGateBlockedTask != nil {
		if recoveryTask, ok, err := s.projectStrategicLeadRecoveryTaskForGate(ctx, workspaceID, agentID, tasks, taskDependencyBlocks, busyTasks, *firstProjectGateBlockedTask, firstProjectGateBlockedPacket); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.HasWork = true
			result.Reason = "project_strategic_lead_recovery"
			taskCopy := *recoveryTask
			result.Task = &taskCopy
			result.ClaimAction = claimActionForAgentWork(taskCopy, agentID)
			result.SessionAction = "start_new"
			result.ResumeSummary = "Recover active strategic lead lease before resuming implementation-gated project work."
			if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
				return AgentWorkNextResult{}, err
			}
			s.attachAgentWorkPacket(ctx, &result, filter)
			return result, nil
		}
		result.Reason = "project_gate_closed"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectGateBlockedTask)
		if filter.IncludePacket {
			result.Packet = firstProjectGateBlockedPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectGateBlockedTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectValidationArtifactBlockedTask != nil {
		result.Reason = "project_validation_artifact_missing"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectValidationArtifactBlockedTask)
		if filter.IncludePacket {
			result.Packet = firstProjectValidationArtifactBlockedPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectValidationArtifactBlockedTask, result.Packet)
		}
		return result, nil
	}
	if firstTaskDependencyBlockedTask != nil {
		result.Reason = "task_dependency_blocked"
		attachAgentWorkResultTaskProjectDigest(&result, *firstTaskDependencyBlockedTask)
		if filter.IncludePacket {
			result.Packet = firstTaskDependencyBlockedPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstTaskDependencyBlockedTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectClaimScopeBusyTask != nil {
		result.Reason = "project_claim_scope_busy"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectClaimScopeBusyTask)
		if filter.IncludePacket {
			result.Packet = firstProjectClaimScopeBusyPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectClaimScopeBusyTask, result.Packet)
		}
		if err := s.attachAgentWorkHydration(ctx, &result, filter); err != nil {
			return AgentWorkNextResult{}, err
		}
		return result, nil
	}
	if firstProjectClaimAdmissionUnclaimableTask != nil {
		result.Reason = "project_claim_admission_unclaimable"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectClaimAdmissionUnclaimableTask)
		if filter.IncludePacket {
			result.Packet = firstProjectClaimAdmissionUnclaimablePacket
		}
		return result, nil
	}
	if firstProjectClaimRepairLeadRequiredTask != nil {
		result.Reason = "project_claim_repair_lead_required"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectClaimRepairLeadRequiredTask)
		if filter.IncludePacket {
			result.Packet = firstProjectClaimRepairLeadRequiredPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectClaimRepairLeadRequiredTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectOwnerBoundTask != nil {
		result.Reason = strings.TrimSpace(firstProjectOwnerBoundPacket.WorkType)
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectOwnerBoundTask)
		if filter.IncludePacket {
			result.Packet = firstProjectOwnerBoundPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectOwnerBoundTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectRoleLaneRequiredTask != nil {
		result.Reason = "project_role_lane_required"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectRoleLaneRequiredTask)
		if filter.IncludePacket {
			result.Packet = firstProjectRoleLaneRequiredPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectRoleLaneRequiredTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectPatchQueueReviewRoleRequiredTask != nil {
		result.Reason = "project_patch_queue_review_role_required"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectPatchQueueReviewRoleRequiredTask)
		if filter.IncludePacket {
			result.Packet = firstProjectPatchQueueReviewRoleRequiredPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectPatchQueueReviewRoleRequiredTask, result.Packet)
		}
		return result, nil
	}
	if firstProjectTargetedDelegationTask != nil {
		result.Reason = "project_targeted_delegation_required"
		attachAgentWorkResultTaskProjectDigest(&result, *firstProjectTargetedDelegationTask)
		if filter.IncludePacket {
			result.Packet = firstProjectTargetedDelegationPacket
			s.attachProjectCoordinationToAgentWork(ctx, &result, firstProjectTargetedDelegationTask, result.Packet)
		}
		return result, nil
	}
	if allowAutonomousExecution {
		if packet, ok, err := s.agentWorkPatchQueueDecisionContinuationAvailable(ctx, workspaceID, agentID); err != nil {
			return AgentWorkNextResult{}, err
		} else if ok {
			result.Reason = "project_patch_queue_decision_continuation_pending"
			result.ProjectID = strings.TrimSpace(packet.ProjectID)
			result.TaskKind = strings.TrimSpace(packet.TaskKind)
			result.ProjectLane = strings.TrimSpace(packet.ProjectLane)
			result.RequiresProjectGate = false
			if filter.IncludePacket {
				result.Packet = packet
			}
			return result, nil
		}
	}

	if !allowAutonomousExecution && agentProfileGateWouldBlockWork(tasks, agentID, agentSessionTasks, pausedTasks, busyTasks) {
		result.Reason = "profile_gate_closed"
		result.ProfileGateBlockedWork = true
		s.attachAgentWorkPacket(ctx, &result, filter)
	}

	return result, nil
}

func agentProfileGateWouldBlockWork(tasks []WorkspaceTaskRecord, agentID string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}) bool {
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) || !isResumableClaimForAgent(task, agentID) {
			continue
		}
		if _, hasSession := agentSessionTasks[task.TaskID]; hasSession {
			continue
		}
		if _, paused := pausedTasks[task.TaskID]; paused {
			continue
		}
		return true
	}
	for _, task := range tasks {
		if task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
			continue
		}
		if _, busy := busyTasks[task.TaskID]; busy {
			continue
		}
		return true
	}
	return false
}

func (s *Store) selectAgentWorkStrategicLeadAuthorityTransition(ctx context.Context, workspaceID, agentID string, activeTask WorkspaceTaskRecord, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks map[string]struct{}) (*WorkspaceTaskRecord, bool, error) {
	if !agentWorkActiveTaskMayYieldForStrategicLeadAuthority(activeTask) {
		return nil, false, nil
	}
	activeProjectID := strings.TrimSpace(activeTask.ProjectID)
	if activeProjectID == "" {
		return nil, false, nil
	}
	for i := range tasks {
		task := tasks[i]
		if !projectStrategicLeadCoordinationTask(task) || strings.TrimSpace(task.ProjectID) != activeProjectID {
			continue
		}
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if _, hasSession := agentSessionTasks[strings.TrimSpace(task.TaskID)]; hasSession {
			continue
		}
		if _, paused := pausedTasks[strings.TrimSpace(task.TaskID)]; paused {
			continue
		}
		if !claimAvailable(task.ClaimStatus) && !isResumableClaimForAgent(task, agentID) {
			continue
		}
		mode, err := s.agentProjectClaimRepairSelectionMode(ctx, workspaceID, agentID, task)
		if err != nil {
			return nil, false, err
		}
		if mode != projectClaimRepairSelectionLead {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			continue
		}
		return &tasks[i], true, nil
	}
	return nil, false, nil
}

func projectStrategicLeadAuthorityTransitionReason(task WorkspaceTaskRecord) string {
	if projectRoleScopeTask(task) {
		return "project_role_scope_authority_transition"
	}
	if projectClaimRepairTask(task) {
		return "project_claim_repair_authority_transition"
	}
	if projectRepositoryRepairTask(task) {
		return "project_repository_repair_authority_transition"
	}
	return "project_strategic_lead_authority_transition"
}

func (s *Store) selectAgentWorkABPCRecoverySuccessor(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, activeTask WorkspaceTaskRecord, tasks []WorkspaceTaskRecord, sessions []AgentSessionStateRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks map[string]struct{}, trustFirst bool) (*WorkspaceTaskRecord, bool, error) {
	if !agentWorkActiveTaskMayYieldForABPCRecoverySuccessor(activeTask) {
		return nil, false, nil
	}
	activeTaskID := strings.TrimSpace(activeTask.TaskID)
	activeProjectID := strings.TrimSpace(activeTask.ProjectID)
	for i := range tasks {
		task := tasks[i]
		recovery, ok := agentWorkABPCRecoveryAction(task)
		if !ok {
			continue
		}
		if strings.TrimSpace(recovery.ClassificationTaskID) != activeTaskID {
			continue
		}
		if activeProjectID != "" && strings.TrimSpace(task.ProjectID) != activeProjectID {
			continue
		}
		if !agentWorkTaskOpenForABPCRecoverySelection(task, agentID, sessions, agentSessionTasks, pausedTasks) {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			continue
		}
		if ok, err := s.agentMaySelectABPCRecoveryAction(ctx, workspaceID, agentID, profile, activeTask, task, recovery, trustFirst); err != nil || !ok {
			return nil, false, err
		}
		return &tasks[i], true, nil
	}
	return nil, false, nil
}

func agentWorkActiveTaskMayYieldForABPCRecoverySuccessor(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-side-effect-classify-") {
		return true
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(workspaceTaskPointerValue(task.ClaimSummary))), "waiting_on_side_effect_resolution_successors") {
		return true
	}
	return false
}

func agentWorkTaskOpenForABPCRecoverySelection(task WorkspaceTaskRecord, agentID string, sessions []AgentSessionStateRecord, agentSessionTasks, pausedTasks map[string]struct{}) bool {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" || isTerminalTaskStatus(task.Status) {
		return false
	}
	if _, hasSession := agentSessionTasks[taskID]; hasSession {
		return false
	}
	if _, paused := pausedTasks[taskID]; paused {
		return false
	}
	if agentWorkTaskBusyByPeerSession(taskID, agentID, sessions) && !isResumableClaimForAgent(task, agentID) {
		return false
	}
	if isResumableClaimForAgent(task, agentID) {
		return true
	}
	return task.Status == model.TaskStatusPending && claimAvailable(task.ClaimStatus)
}

func agentWorkTaskBusyByPeerSession(taskID, agentID string, sessions []AgentSessionStateRecord) bool {
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if taskID == "" {
		return false
	}
	for _, session := range sessions {
		if strings.TrimSpace(session.TaskID) != taskID || strings.TrimSpace(session.AgentID) == agentID {
			continue
		}
		if isEndedAgentWorkSessionStatus(session.Status) {
			continue
		}
		return true
	}
	return false
}

func (s *Store) agentMaySelectABPCRecoveryAction(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, activeTask, task WorkspaceTaskRecord, recovery agentWorkABPCRecoveryActionInfo, trustFirst bool) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false, nil
	}
	if strings.TrimSpace(recovery.TargetAgentID) == agentID || strings.TrimSpace(recovery.OwnerAgentID) == agentID {
		return true, nil
	}
	if strings.TrimSpace(recovery.ClassificationTaskID) != "" && strings.TrimSpace(recovery.ClassificationTaskID) == strings.TrimSpace(activeTask.TaskID) {
		return true, nil
	}
	if projectID := strings.TrimSpace(task.ProjectID); projectID != "" {
		if lead, ok, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID); err != nil {
			return false, err
		} else if ok && strings.TrimSpace(lead.AgentID) == agentID {
			return true, nil
		}
	}
	return agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst), nil
}

func agentWorkActiveTaskMayYieldForStrategicLeadAuthority(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || projectRoleScopeTask(task) {
		return false
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(task.ProjectLane)) {
	case "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing", "coordination":
		return true
	default:
		return false
	}
}

func agentProfileAllowsFreshTaskSelection(profile AgentProfileRecord, task WorkspaceTaskRecord) bool {
	if agentWorkTaskRequiresStrategyProfile(task) {
		return agentProfileAllowsStrategyTaskSelection(profile)
	}
	switch agentProfileFreshSelectionMode(profile) {
	case "review":
		return agentWorkTaskLooksReviewScoped(task)
	case "synthesis":
		return agentWorkTaskLooksSynthesisScoped(task) || agentWorkTaskLooksValidationScoped(task)
	case "strategy":
		return agentWorkTaskLooksStrategyScoped(task)
	case "implementation":
		return agentWorkTaskLooksImplementationScoped(task) || agentWorkTaskLooksValidationScoped(task)
	default:
		return true
	}
}

func agentProfileAllowsFreshTaskSelectionForMode(profile AgentProfileRecord, task WorkspaceTaskRecord, trustFirst bool) bool {
	if agentWorkTaskRequiresStrategyProfile(task) {
		return agentProfileAllowsStrategyTaskSelection(profile)
	}
	if !trustFirst {
		return agentProfileAllowsFreshTaskSelection(profile, task)
	}
	if agentWorkTaskIsProactiveMetacognition(task) {
		return agentProfileAllowsProactiveMetacognitionTask(profile, task)
	}
	if agentWorkTaskIsPureImplementationSelection(task) {
		switch agentProfileFreshSelectionMode(profile) {
		case "review", "synthesis", "strategy":
			return false
		}
	}
	if agentWorkTaskIsPureStrategySelection(task) {
		switch agentProfileFreshSelectionMode(profile) {
		case "review", "synthesis", "implementation":
			return false
		}
	}
	return true
}

func agentWorkTaskRequiresStrategyProfile(task WorkspaceTaskRecord) bool {
	if projectStrategicLeadCoordinationTask(task) || agentWorkTaskIsProactiveMetacognition(task) {
		return false
	}
	if agentWorkProjectLaneRequiresStrategyProfile(task.ProjectLane) {
		return true
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return true
	}
	return false
}

func (s *Store) selectAgentWorkPendingOperatorRoot(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, busyTasks map[string]struct{}, trustFirst bool) (*WorkspaceTaskRecord, bool, error) {
	for i := range tasks {
		task := tasks[i]
		if !agentWorkTaskLooksOperatorSpecRoot(task) {
			continue
		}
		if !agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst) {
			continue
		}
		if task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
			continue
		}
		if _, busy := busyTasks[strings.TrimSpace(task.TaskID)]; busy {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked || packet != nil && packet.Gate != nil && strings.TrimSpace(packet.Gate.GateState) == "closed" {
			continue
		}
		if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil {
			return nil, false, err
		} else if blocked || packet != nil && strings.TrimSpace(packet.WorkType) != "" {
			continue
		}
		if !agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, task) {
			if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil {
				return nil, false, err
			} else if !ok {
				continue
			}
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked || packet != nil && packet.Gate != nil && strings.TrimSpace(packet.Gate.GateState) == "closed" {
			continue
		}
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked || packet != nil && packet.Gate != nil && strings.TrimSpace(packet.Gate.GateState) == "closed" {
			continue
		}
		return &tasks[i], true, nil
	}
	return nil, false, nil
}

func agentWorkProjectLaneRequiresStrategyProfile(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing":
		return true
	default:
		return false
	}
}

func agentProfileAllowsStrategyTaskSelection(profile AgentProfileRecord) bool {
	mode := agentProfileFreshSelectionMode(profile)
	if mode == "strategy" {
		return true
	}
	if agentProfileHasStrategyMandate(profile) && !agentProfileHasArtifactReflectionScopeMetadata(profile) {
		return true
	}
	if mode != "generalist" {
		return false
	}
	if agentProfileHasImplementationMandate(profile) || agentProfileHasHighConfidenceReviewSignal(profile) {
		return false
	}
	return !agentProfileHasExplicitArtifactReflectionScope(profile)
}

func agentProfileHasStrategyMandate(profile AgentProfileRecord) bool {
	return agentProfileHasStrictSelectionSignal(profile, []string{
		"strategist",
		"product coordinator",
		"project coordinator",
		"autonomous product coordinator",
		"project strategy",
		"autonomous project strategy",
		"task decomposition",
		"root task",
		"project framing",
		"shared design docs",
		"coordination plan",
	})
}

func agentProfileHasExplicitArtifactReflectionScope(profile AgentProfileRecord) bool {
	if agentProfileHasArtifactBoundMetacognitionSignal(profile) {
		return true
	}
	return agentProfileHasArtifactReflectionScopeMetadata(profile)
}

func agentProfileHasArtifactReflectionScopeMetadata(profile AgentProfileRecord) bool {
	scope := strings.ToLower(strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "reflection_scope")))
	scope = strings.ReplaceAll(scope, "-", "_")
	scope = strings.ReplaceAll(scope, " ", "_")
	switch scope {
	case "artifact", "artifact_review", "review", "qa":
		return true
	default:
		return false
	}
}

func (s *Store) agentWorkMayBypassFreshProfileGate(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	if bypass, err := s.agentWorkRequiredOwnerMayBypassFreshProfileGate(ctx, workspaceID, agentID, task); err != nil || bypass {
		return bypass, err
	}
	if bypass, err := s.agentWorkActiveProjectRoleMayBypassFreshProfileGate(ctx, workspaceID, agentID, task); err != nil || bypass {
		return bypass, err
	}
	return s.agentWorkStrategicLeadCoordinationMayBypassFreshProfileGate(ctx, workspaceID, agentID, task)
}

func (s *Store) agentWorkRequiredOwnerMayBypassFreshProfileGate(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
	if err != nil || !ok {
		return false, err
	}
	if req.RepairNeeded {
		return false, nil
	}
	kind := strings.TrimSpace(req.Kind)
	if !strings.EqualFold(kind, "patch_queue_submit") && !strings.EqualFold(kind, "patch_queue_revision") {
		return false, nil
	}
	if strings.TrimSpace(req.BranchID) == "" || strings.TrimSpace(req.RequiredAgentID) == "" {
		return false, nil
	}
	return strings.TrimSpace(agentID) == strings.TrimSpace(req.RequiredAgentID), nil
}

func (s *Store) agentWorkActiveProjectRoleMayBypassFreshProfileGate(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	requiredRoles := projectClaimRequiredRoleTypesForLane(task.ProjectLane)
	projectID := strings.TrimSpace(task.ProjectID)
	agentID = strings.TrimSpace(agentID)
	if projectID == "" || agentID == "" || len(requiredRoles) == 0 {
		return false, nil
	}
	roles, err := s.ListProjectRoles(ctx, workspaceID, projectID, false)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		roleType := strings.ToUpper(strings.TrimSpace(role.RoleType))
		if roleType == ProjectRoleStrategicLead {
			continue
		}
		if strings.TrimSpace(role.AgentID) != agentID || !projectClaimRoleTypeAllowed(roleType, requiredRoles) {
			continue
		}
		if roleType == ProjectRoleImplementer && len(writeScopePaths(role.WriteScopeJSON)) == 0 {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (s *Store) agentWorkStrategicLeadCoordinationMayBypassFreshProfileGate(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	if !projectStrategicLeadCoordinationTask(task) {
		return false, nil
	}
	return s.agentMaySelectProjectClaimRepairTask(ctx, workspaceID, agentID, task)
}

func agentWorkTaskIsPureImplementationSelection(task WorkspaceTaskRecord) bool {
	if !agentWorkTaskLooksImplementationScoped(task) {
		return false
	}
	return !agentWorkTaskLooksReviewScoped(task) &&
		!agentWorkTaskLooksValidationScoped(task) &&
		!agentWorkTaskLooksSynthesisScoped(task) &&
		!agentWorkTaskLooksStrategyScoped(task)
}

func agentWorkTaskIsPureStrategySelection(task WorkspaceTaskRecord) bool {
	if !agentWorkTaskLooksStrategyScoped(task) {
		return false
	}
	return !agentWorkTaskLooksReviewScoped(task) &&
		!agentWorkTaskLooksValidationScoped(task) &&
		!agentWorkTaskLooksSynthesisScoped(task) &&
		!agentWorkTaskLooksImplementationScoped(task)
}

func agentWorkTaskIsProactiveMetacognition(task WorkspaceTaskRecord) bool {
	if strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection") {
		return true
	}
	if agentWorkTaskHasTag(task, "idle-reflection", "meta-reflection", "anti-idle") ||
		agentWorkTaskHasTagPrefix(task, "metacognition-scope-", "idle-policy-") {
		return true
	}
	text := agentWorkTaskMetacognitionSearchText(task)
	return agentWorkTextContainsAny(text, []string{
		"meta-reflection",
		"metacognition pass",
		"metacognition reflection",
		"metacognitive reflection",
		"reflection pass",
	})
}

func agentProfileAllowsProactiveMetacognitionTask(profile AgentProfileRecord, task WorkspaceTaskRecord) bool {
	if !agentProfileCanOpenReflectionTasks(profile) {
		return false
	}
	profileScope := agentProfileReflectionScope(profile)
	taskScope := agentWorkTaskMetacognitionScope(task)
	switch profileScope {
	case "local":
		return false
	case "artifact":
		if taskScope != "" {
			return taskScope == "artifact"
		}
		return agentWorkTaskLooksReviewScoped(task) || agentWorkTaskLooksValidationScoped(task)
	case "project":
		return true
	case "global":
		return true
	default:
		return agentProfileAllowsFreshTaskSelection(profile, task)
	}
}

func agentProfileCanOpenReflectionTasks(profile AgentProfileRecord) bool {
	if value, ok := agentProfileMetadataBool(profile.Metadata, "can_open_reflection_tasks"); ok {
		if !value && agentWorkReflectionScopeRank(agentProfileSemanticReflectionScope(profile)) >= agentWorkReflectionScopeRank("project") {
			return true
		}
		return value
	}
	return agentProfileReflectionScope(profile) != "local"
}

func agentProfileReflectionScope(profile AgentProfileRecord) string {
	explicitScope := ""
	scope := strings.ToLower(strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "reflection_scope")))
	scope = strings.ReplaceAll(scope, "-", "_")
	scope = strings.ReplaceAll(scope, " ", "_")
	switch scope {
	case "local", "task", "own_task", "lane":
		explicitScope = "local"
	case "artifact", "artifact_review", "review", "qa":
		explicitScope = "artifact"
	case "project", "project_wide", "projectwide":
		explicitScope = "project"
	case "global", "workspace", "system", "systemic":
		explicitScope = "global"
	}
	if semanticScope := agentProfileSemanticReflectionScope(profile); semanticScope != "" {
		if explicitScope == "" || agentWorkReflectionScopeRank(semanticScope) > agentWorkReflectionScopeRank(explicitScope) {
			return semanticScope
		}
	}
	if explicitScope != "" {
		return explicitScope
	}
	switch agentProfileFreshSelectionMode(profile) {
	case "implementation":
		return "local"
	case "review":
		return "artifact"
	case "strategy", "synthesis":
		return "project"
	default:
		return "artifact"
	}
}

func agentProfileSemanticReflectionScope(profile AgentProfileRecord) string {
	if agentProfileHasArtifactBoundMetacognitionSignal(profile) {
		return "artifact"
	}
	text := agentProfileSemanticSignalText(profile)
	if text == "" {
		return ""
	}
	switch {
	case agentWorkTextContainsAny(text, []string{
		"global meta-cognition",
		"global metacognition",
		"global strategy",
		"workspace strategy",
		"workspace steward",
		"system steward",
		"systemic steward",
		"systemic observer",
		"meta-analysis",
		"meta analysis",
		"portfolio",
		"venture",
		"service factory",
		"service-factory",
		"market scout",
		"opportunity",
		"growth",
		"monetization",
		"advertising",
		"revenue",
		"governance",
	}):
		return "global"
	case agentWorkTextContainsAny(text, []string{
		"strategist",
		"strategy",
		"strategic",
		"planner",
		"planning",
		"project lead",
		"lead strategist",
		"coordinator",
		"coordination",
		"architecture",
		"architect",
		"integrator",
		"integration",
		"synthesis",
		"synthesizer",
		"handoff",
		"finalization",
		"release",
		"deploy",
		"deployment",
		"ops",
	}):
		return "project"
	case agentWorkTextContainsAny(text, []string{
		"ui/ux",
		"ui ux",
		"user experience",
		"ux critic",
		"reviewer",
		"review",
		"qa",
		"tester",
		"testing",
		"verifier",
		"validation",
		"accessibility",
		"usability",
	}):
		return "artifact"
	case agentWorkTextContainsAny(text, []string{
		"implementer",
		"implementation",
		"builder",
		"worker",
		"frontend",
		"backend",
		"fullstack",
		"full-stack",
		"coder",
	}):
		return "local"
	default:
		return ""
	}
}

func agentProfileHasArtifactBoundMetacognitionSignal(profile AgentProfileRecord) bool {
	freeText := strings.ToLower(strings.Join([]string{
		profile.Specialization,
		agentProfileMetadataString(profile.Metadata, "default_work_mode"),
		agentProfileMetadataString(profile.Metadata, "primary_specialization"),
	}, " "))
	if agentWorkTextContainsAny(freeText, []string{
		"ui/ux",
		"ui ux",
		"user experience",
		"ux critic",
		"reviewer",
		"review",
		"qa",
		"tester",
		"testing",
		"verifier",
		"validation",
		"accessibility",
		"usability",
	}) {
		return true
	}
	for _, tag := range profile.Tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		switch tag {
		case "ui/ux", "ui-ux", "ux", "ux-review", "ux-qa", "ux-critic", "reviewer", "qa", "tester", "testing", "verifier", "validation", "accessibility", "usability":
			return true
		}
		if strings.Contains(tag, "user-experience") || strings.Contains(tag, "ux-critic") {
			return true
		}
	}
	return false
}

func agentProfileSemanticSignalText(profile AgentProfileRecord) string {
	parts := []string{
		profile.AgentID,
		profile.Bio,
		profile.Specialization,
		strings.Join(profile.Tags, " "),
		agentProfileMetadataString(profile.Metadata, "default_work_mode"),
		agentProfileMetadataString(profile.Metadata, "primary_specialization"),
		agentProfileMetadataString(profile.Metadata, "mission"),
	}
	for _, key := range []string{"secondary_specializations", "domain_scope", "autonomous_objectives", "service_factory_focus"} {
		parts = append(parts, agentProfileMetadataStringList(profile.Metadata, key)...)
	}
	return strings.ToLower(strings.TrimSpace(strings.Join(parts, " ")))
}

func agentProfileMetadataStringList(metadata map[string]any, key string) []string {
	if len(metadata) == 0 {
		return nil
	}
	value, ok := metadata[strings.TrimSpace(key)]
	if !ok {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			switch v := item.(type) {
			case string:
				out = append(out, v)
			case fmt.Stringer:
				out = append(out, v.String())
			}
		}
		return out
	case string:
		return []string{typed}
	default:
		return nil
	}
}

func agentWorkReflectionScopeRank(scope string) int {
	switch normalizeAgentWorkMetacognitionScope(scope) {
	case "local":
		return 0
	case "artifact":
		return 1
	case "project":
		return 2
	case "global":
		return 3
	default:
		return -1
	}
}

func agentWorkTaskMetacognitionScope(task WorkspaceTaskRecord) string {
	for _, tag := range task.Tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if strings.HasPrefix(tag, "metacognition-scope-") {
			if scope := normalizeAgentWorkMetacognitionScope(strings.TrimPrefix(tag, "metacognition-scope-")); scope != "" {
				return scope
			}
		}
	}
	text := agentWorkTaskMetacognitionSearchText(task)
	switch {
	case agentWorkTextContainsAny(text, []string{"global metacognition", "global reflection", "system metacognition", "systemic reflection", "workspace metacognition"}):
		return "global"
	case agentWorkTextContainsAny(text, []string{"project metacognition", "project reflection", "project-wide reflection", "project wide reflection"}):
		return "project"
	case agentWorkTextContainsAny(text, []string{"artifact metacognition", "artifact reflection", "artifact quality iteration", "review reflection", "qa reflection"}):
		return "artifact"
	case strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection") && strings.TrimSpace(task.ProjectID) != "":
		return "project"
	case strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection"):
		return "global"
	default:
		return ""
	}
}

func agentWorkTaskMetacognitionSearchText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskKind,
		task.TaskTemplate,
		task.TaskClass,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, " "))
}

func normalizeAgentWorkMetacognitionScope(scope string) string {
	scope = strings.ToLower(strings.TrimSpace(scope))
	scope = strings.ReplaceAll(scope, "-", "_")
	scope = strings.ReplaceAll(scope, " ", "_")
	switch scope {
	case "local", "task", "own_task", "lane":
		return "local"
	case "artifact", "artifact_review", "review", "qa":
		return "artifact"
	case "project", "project_wide", "projectwide":
		return "project"
	case "global", "workspace", "system", "systemic":
		return "global"
	default:
		return ""
	}
}

// agentWorkTaskSuperseded reports whether a durable receipt already terminalizes this
// task. It is intentionally a thin alias over the receipt registry
// (agentWorkRequiredTerminalReceipt) so the supersession answer can never drift from the
// sweep's answer when a new receipt kind is added (F12: the previous hand-maintained
// chain here duplicated every registry predicate one-by-one).
func (s *Store) agentWorkTaskSuperseded(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	_, ok, err := s.agentWorkRequiredTerminalReceipt(ctx, workspaceID, task)
	return ok, err
}

func (s *Store) agentWorkResolvedPatchQueueClaimStewardshipTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if !agentWorkPatchQueueClaimStewardshipTask(task) {
		return false, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if strings.TrimSpace(queueID) == "" || strings.TrimSpace(itemID) == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(branchID),
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) != strings.TrimSpace(queueID) || strings.TrimSpace(item.ItemID) != strings.TrimSpace(itemID) {
			continue
		}
		return strings.ToUpper(strings.TrimSpace(item.State)) != ProjectPatchQueueStateClaimed, nil
	}
	return false, nil
}

func (s *Store) filterSupersededAgentWorkDependencyBlocks(ctx context.Context, workspaceID string, taskIndex map[string]WorkspaceTaskRecord, blocks map[string][]string) (map[string][]string, error) {
	if len(blocks) == 0 {
		return blocks, nil
	}
	out := make(map[string][]string, len(blocks))
	for taskID, dependencyTaskIDs := range blocks {
		taskID = strings.TrimSpace(taskID)
		for _, dependencyTaskID := range dependencyTaskIDs {
			dependencyTaskID = strings.TrimSpace(dependencyTaskID)
			if dependencyTaskID == "" {
				continue
			}
			dependencyTask, ok := taskIndex[dependencyTaskID]
			if !ok {
				out[taskID] = append(out[taskID], dependencyTaskID)
				continue
			}
			superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, dependencyTask)
			if err != nil {
				return nil, err
			}
			if superseded {
				continue
			}
			targetTask, hasTargetTask := taskIndex[taskID]
			if hasTargetTask {
				artifactReady, err := s.agentWorkDependencySatisfiedByPublishedPatchQueueArtifact(ctx, workspaceID, targetTask, dependencyTask)
				if err != nil {
					return nil, err
				}
				if artifactReady {
					continue
				}
			}
			out[taskID] = append(out[taskID], dependencyTaskID)
		}
	}
	return out, nil
}

func (s *Store) agentWorkDependencySatisfiedByPublishedPatchQueueArtifact(ctx context.Context, workspaceID string, targetTask, dependencyTask WorkspaceTaskRecord) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID := strings.TrimSpace(targetTask.ProjectID)
	if workspaceID == "" || projectID == "" || !strings.EqualFold(projectID, strings.TrimSpace(dependencyTask.ProjectID)) {
		return false, nil
	}
	if !agentWorkPatchQueueEvidenceTask(targetTask) || !projectTaskRequiresImplementationGate(dependencyTask) {
		return false, nil
	}
	if artifactReady, err := s.agentWorkTargetTaskNamesPublishedPatchQueueArtifact(ctx, workspaceID, projectID, targetTask); err != nil || artifactReady {
		return artifactReady, err
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		IncludeInactive: true,
	})
	if err != nil {
		return false, err
	}
	dependencyBranchID := workspaceTaskPointerValue(dependencyTask.ClaimBranchID)
	for _, branch := range branches {
		if !agentWorkBranchBelongsToDependencyTask(branch, dependencyTask, dependencyBranchID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(branch.Status), ProjectBranchStatusReadyForReview) {
			continue
		}
		items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
			WorkspaceID: workspaceID,
			ProjectID:   projectID,
			BranchID:    strings.TrimSpace(branch.BranchID),
		})
		if err != nil {
			return false, err
		}
		for _, item := range items {
			if !agentWorkPatchQueueStateIsPublishedEvidence(item.State) {
				continue
			}
			if agentWorkTaskMatchesPatchQueueArtifact(targetTask, branch, item) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Store) agentWorkTargetTaskNamesPublishedPatchQueueArtifact(ctx context.Context, workspaceID, projectID string, targetTask WorkspaceTaskRecord) (bool, error) {
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(projectID),
	})
	if err != nil {
		return false, err
	}
	if len(items) == 0 {
		return false, nil
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       strings.TrimSpace(projectID),
		IncludeInactive: true,
	})
	if err != nil {
		return false, err
	}
	branchesByID := make(map[string]ProjectBranchRecord, len(branches))
	for _, branch := range branches {
		branchesByID[strings.TrimSpace(branch.BranchID)] = branch
	}
	for _, item := range items {
		if !agentWorkPatchQueueStateIsPublishedEvidence(item.State) {
			continue
		}
		branch := branchesByID[strings.TrimSpace(item.BranchID)]
		if agentWorkTaskMatchesPatchQueueArtifact(targetTask, branch, item) {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueueEvidenceTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkProjectLaneIsReview(task.ProjectLane) {
		return false
	}
	if agentWorkPatchQueueSupersedeStewardshipTask(task) || agentWorkPatchQueueClaimStewardshipTask(task) {
		return false
	}
	patchQueueSignal := agentWorkTaskHasTag(task, "patch_queue", "patch-queue")
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	patchQueueSignal = patchQueueSignal ||
		strings.Contains(text, "patch queue") ||
		strings.Contains(text, "patch-queue") ||
		strings.Contains(text, "patchitem-") ||
		strings.Contains(text, "project_patch_queue")
	if !patchQueueSignal {
		return false
	}
	if agentWorkProjectLaneIsValidation(task.ProjectLane) {
		return true
	}
	if agentWorkTaskHasTag(task, "validation", "visual-qa", "qa", "test", "testing", "verification", "acceptance", "browser-smoke", "smoke", "evidence") {
		return true
	}
	for _, marker := range []string{
		"evidence",
		"validate",
		"validation",
		"verify",
		"verification",
		"visual acceptance",
		"browser smoke",
		"smoke test",
		"acceptance artifact",
		"materialize missing",
		"rhizome_visual_acceptance",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func agentWorkBranchBelongsToDependencyTask(branch ProjectBranchRecord, dependencyTask WorkspaceTaskRecord, dependencyBranchID string) bool {
	if dependencyBranchID != "" && strings.EqualFold(strings.TrimSpace(branch.BranchID), dependencyBranchID) {
		return true
	}
	taskID := strings.TrimSpace(dependencyTask.TaskID)
	if taskID == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(branch.ActiveTaskID), taskID)
}

func agentWorkPatchQueueStateIsPublishedEvidence(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed, ProjectPatchQueueStateBlocked, ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated:
		return true
	default:
		return false
	}
}

func agentWorkTaskMatchesPatchQueueArtifact(task WorkspaceTaskRecord, branch ProjectBranchRecord, item ProjectPatchQueueItemRecord) bool {
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if queueID != "" || itemID != "" || branchID != "" {
		if queueID != "" && !strings.EqualFold(queueID, strings.TrimSpace(item.QueueID)) {
			return false
		}
		if itemID != "" && !strings.EqualFold(itemID, strings.TrimSpace(item.ItemID)) {
			return false
		}
		if branchID != "" && !strings.EqualFold(branchID, strings.TrimSpace(branch.BranchID)) {
			return false
		}
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	for _, value := range []string{
		item.QueueID,
		item.ItemID,
		item.BranchID,
		item.HeadSHA,
		branch.BranchID,
		branch.BranchName,
		branch.HeadSHA,
	} {
		value = strings.ToLower(strings.TrimSpace(value))
		if len(value) >= 8 && strings.Contains(text, value) {
			return true
		}
	}
	if agentWorkPatchQueueTaskHasCompatibleHeadToken(task, item, branch) {
		return true
	}
	return false
}

func (s *Store) agentWorkTerminalPatchQueueReviewTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if agentWorkPatchQueueSupersedeStewardshipTask(task) {
		return false, nil
	}
	if agentWorkPatchQueueEvidenceTask(task) {
		return false, nil
	}
	if !agentWorkPatchQueueReviewTask(task) {
		return false, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if queueID == "" && itemID == "" && branchID == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    branchID,
	})
	if err != nil {
		return false, err
	}
	terminalMatch := false
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		if branchID != "" && strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed:
			return false, nil
		default:
			terminalMatch = true
		}
	}
	return terminalMatch, nil
}

func (s *Store) agentWorkReleasedBlockedOwnerSubmitHandoffTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasOwnerBoundSignal(task) {
		return false, nil
	}
	if !strings.EqualFold(workspaceTaskPointerValue(task.ClaimStatus), model.TaskClaimStatusReleased) {
		return false, nil
	}
	req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
	if err != nil || !ok {
		return false, err
	}
	if req.RepairNeeded || !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") {
		return false, nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		  FROM runtime_events
		 WHERE workspace_id = ?
		   AND task_id = ?
		   AND event_type = 'task.blocked'
		   AND entity_type = 'task'
		   AND entity_id = ?`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.TaskID),
	).Scan(&count); err != nil {
		return false, fmt.Errorf("check released blocked owner-submit handoff supersession: %w", err)
	}
	return count > 0, nil
}

func (s *Store) agentWorkStaleOwnerSubmitTerminalAcceptedTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasOwnerBoundSignal(task) {
		return false, nil
	}
	req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
	if err != nil || !ok {
		return false, err
	}
	if req.RepairNeeded || !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") || strings.TrimSpace(req.BranchID) == "" {
		return false, nil
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       strings.TrimSpace(task.ProjectID),
		IncludeInactive: true,
	})
	if err != nil {
		return false, err
	}
	var matchedBranch ProjectBranchRecord
	foundBranch := false
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != strings.TrimSpace(req.BranchID) {
			continue
		}
		matchedBranch = branch
		foundBranch = true
		break
	}
	if !foundBranch || !projectBranchStatusIsTerminal(matchedBranch.Status) {
		return false, nil
	}
	headSHA := strings.TrimSpace(matchedBranch.HeadSHA)
	if headSHA == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(matchedBranch.BranchID),
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated:
		default:
			continue
		}
		if strings.TrimSpace(item.HeadSHA) == headSHA {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) agentWorkStaleProjectClaimRepairTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if !projectClaimRepairTask(task) || strings.TrimSpace(task.ProjectID) == "" {
		return false, nil
	}
	branchID := agentWorkClaimRepairTextFieldValue(task, "conflict_branch_id")
	if branchID == "" {
		// F3: claim-repair sidecars without a conflict branch (the fresh-owner-evidence
		// shape) anchor on the conflict/blocked task instead. The repair is moot once a
		// referenced task is terminal: either the blocked lane finished some other way,
		// or the conflicting owner lane ended and released its scope.
		return s.agentWorkStaleProjectClaimRepairTaskByTaskRefs(ctx, task)
	}
	repoID := agentWorkClaimRepairTextFieldValue(task, "repo_id")
	liveBranches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		RepoID:      repoID,
	})
	if err != nil {
		return false, err
	}
	for _, branch := range liveBranches {
		if strings.TrimSpace(branch.BranchID) == branchID {
			return false, nil
		}
	}
	allBranches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       strings.TrimSpace(task.ProjectID),
		RepoID:          repoID,
		IncludeInactive: true,
	})
	if err != nil {
		return false, err
	}
	for _, branch := range allBranches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		return agentWorkProjectClaimRepairBranchTerminalStatus(branch.Status), nil
	}
	return true, nil
}

// agentWorkStaleProjectClaimRepairTaskByTaskRefs handles claim-repair carriers that do
// not name a conflict branch: it extracts the conflict/blocked task references from the
// carrier description and reports the carrier stale when any referenced task is already
// terminal. Carriers without any task reference are left untouched (fail-closed).
func (s *Store) agentWorkStaleProjectClaimRepairTaskByTaskRefs(ctx context.Context, task WorkspaceTaskRecord) (bool, error) {
	ownTaskID := strings.TrimSpace(task.TaskID)
	for _, key := range []string{"conflict_task_id", "blocked_task_id"} {
		ref := strings.TrimSpace(agentWorkClaimRepairTextFieldValue(task, key))
		if ref == "" || ref == ownTaskID {
			continue
		}
		var status string
		err := s.db.QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, ref).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("query claim repair task ref status: %w", err)
		}
		if isTerminalTaskStatus(status) {
			return true, nil
		}
		// R22-F1: the repair's purpose is to make the blocked task claimable again. When the
		// blocked task now holds an ACTIVE claim (someone claimed it successfully after the
		// conflicting scope cleared), the repair is achieved - terminalize the carrier instead
		// of letting its required-transition gate loop into operator blockers.
		if key == "blocked_task_id" {
			var activeClaims int
			if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM task_claims
 WHERE task_id = ?
   AND UPPER(TRIM(claim_status)) = 'CLAIMED'`,
				ref,
			).Scan(&activeClaims); err != nil {
				return false, fmt.Errorf("query claim repair blocked-task claim state: %w", err)
			}
			if activeClaims > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Store) agentWorkStalePatchQueueReplacementTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false, nil
	}
	if !agentWorkPatchQueueReplacementSupersessionCandidate(task) {
		return false, nil
	}
	staleItem, ok, err := s.agentWorkTerminalPatchQueueItemForTask(ctx, workspaceID, task)
	if err != nil || !ok {
		return false, err
	}
	switch strings.ToUpper(strings.TrimSpace(staleItem.State)) {
	case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
	default:
		return false, nil
	}
	if len(agentWorkPatchQueueItemPathset(staleItem)) == 0 {
		return false, nil
	}
	candidates, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		RepoID:      strings.TrimSpace(staleItem.RepoID),
	})
	if err != nil {
		return false, err
	}
	staleBranch, _, err := s.agentWorkPatchQueueCandidateBranch(ctx, workspaceID, task.ProjectID, staleItem)
	if err != nil {
		return false, err
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.QueueID) == strings.TrimSpace(staleItem.QueueID) &&
			strings.TrimSpace(candidate.ItemID) == strings.TrimSpace(staleItem.ItemID) {
			continue
		}
		if !agentWorkPatchQueueItemsOverlap(candidate, staleItem) {
			continue
		}
		supersedes := agentWorkPatchQueueItemSupersedes(candidate, staleItem)
		if !supersedes && !agentWorkPatchQueueItemDecidedAfter(candidate, staleItem) {
			continue
		}
		candidateBranch, live, err := s.agentWorkPatchQueueCandidateBranch(ctx, workspaceID, task.ProjectID, candidate)
		if err != nil || !live {
			return false, err
		}
		switch strings.ToUpper(strings.TrimSpace(candidate.State)) {
		case ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated:
			return true, nil
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
			if !supersedes && strings.TrimSpace(staleBranch.AgentID) != "" && strings.TrimSpace(candidateBranch.AgentID) != "" &&
				!strings.EqualFold(strings.TrimSpace(staleBranch.AgentID), strings.TrimSpace(candidateBranch.AgentID)) {
				continue
			}
			hasEvidence, err := s.agentWorkPatchQueueVisualEvidenceDocExistsForItem(ctx, workspaceID, candidate, candidateBranch)
			if err != nil {
				return false, err
			}
			if hasEvidence {
				return true, nil
			}
		}
	}
	return false, nil
}

func agentWorkPatchQueueReplacementSupersessionCandidate(task WorkspaceTaskRecord) bool {
	if agentWorkPatchQueueReviewTask(task) {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") {
		return false
	}
	if !agentWorkTaskHasTag(task, "patch_queue", "patch-queue") || !agentWorkTaskHasTag(task, "revision") {
		return false
	}
	return true
}

// agentWorkStalePatchQueueRevisionFollowupBehindTerminalItem (FF-R09-2) reports whether a
// revision follow-up's underlying candidate is already terminal: its referenced item reached
// INTEGRATED/CANCELED (the R57 in-place integrate flow), or the referenced branch carries ANY
// INTEGRATED item (the rev-1-under-new-item flow). Such a follow-up is moot and must cancel
// instead of staying PENDING-selectable - in the S1 rerun R08 one stale revision follow-up
// consumed 14 beta iterations across two sessions without ever terminalizing. Detection uses
// the structural patch_queue_task_kind=revision stamp with the existing text-shape detector as
// fallback for legacy/unstamped follow-ups. Validation/review follow-ups are intentionally
// excluded: post-integration validation is legitimate work, while revising an already
// integrated candidate is definitionally moot.
func (s *Store) agentWorkStalePatchQueueRevisionFollowupBehindTerminalItem(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false, nil
	}
	structuredRevision := strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision")
	if !structuredRevision && !agentWorkPatchQueueRevisionFollowupTask(task) {
		return false, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if queueID != "" && itemID != "" {
		var state string
		err := s.db.QueryRowContext(ctx,
			`SELECT state FROM project_patch_queue_items WHERE queue_id = ? AND item_id = ?`,
			queueID, itemID,
		).Scan(&state)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("query revision followup item state: %w", err)
		}
		if err == nil {
			switch strings.ToUpper(strings.TrimSpace(state)) {
			case ProjectPatchQueueStateIntegrated, ProjectPatchQueueStateCanceled:
				return true, nil
			}
		}
	}
	if branchID == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    branchID,
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.State), ProjectPatchQueueStateIntegrated) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) agentWorkStalePatchQueueValidationSidecarBehindQueueBoundTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkPatchQueueGenericValidationSidecarCandidate(task) {
		return false, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, strings.TrimSpace(task.ProjectID))
	if err != nil {
		return false, err
	}
	branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		if id := strings.TrimSpace(branch.BranchID); id != "" {
			branches[id] = branch
		}
	}
	for _, item := range coordination.PatchQueueItems {
		branch := branches[strings.TrimSpace(item.BranchID)]
		if !agentWorkTaskMatchesPatchQueueArtifact(task, branch, item) || !agentWorkPatchQueueTaskHeadCompatible(task, item, branch) {
			continue
		}
		if agentWorkQueueBoundValidationOrReviewTaskExistsForItem(coordination.Tasks, strings.TrimSpace(task.TaskID), item, branch) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) agentWorkStalePatchQueueValidationSidecarBehindVisualEvidenceDoc(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false, nil
	}
	if !agentWorkPatchQueueGenericValidationSidecarCandidate(task) &&
		!agentWorkPatchQueueDeterministicValidationTask(task) &&
		!agentWorkPatchQueueEvidenceRefreshTask(task) {
		return false, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, strings.TrimSpace(task.ProjectID))
	if err != nil {
		return false, err
	}
	branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		if id := strings.TrimSpace(branch.BranchID); id != "" {
			branches[id] = branch
		}
	}
	for _, item := range coordination.PatchQueueItems {
		branch := branches[strings.TrimSpace(item.BranchID)]
		if !agentWorkTaskMatchesPatchQueueArtifact(task, branch, item) || !agentWorkPatchQueueTaskHeadCompatible(task, item, branch) {
			continue
		}
		if ok, err := s.agentWorkPatchQueueVisualEvidenceDocExistsForItem(ctx, workspaceID, item, branch); err != nil || ok {
			return ok, err
		}
		if ok, err := s.agentWorkPatchQueuePositiveValidationEvidenceDocExistsForItem(ctx, workspaceID, item, branch); err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func (s *Store) agentWorkPatchQueueVisualEvidenceDocExistsForItem(ctx context.Context, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) (bool, error) {
	summaries, err := s.ListWorkspaceDocs(ctx, workspaceID, false)
	if err != nil {
		return false, err
	}
	for _, summary := range summaries {
		if !agentWorkPatchQueueVisualEvidenceSummaryCouldMatch(summary, item, branch) {
			continue
		}
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, summary.DocKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return false, err
		}
		if doc.ArchivedAt != nil {
			continue
		}
		if agentWorkPatchQueueVisualEvidenceDocNamesItem(doc, item, branch) && agentWorkPatchQueueVisualEvidenceDocFreshForItem(doc, item) {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueueVisualEvidenceSummaryCouldMatch(doc WorkspaceDocSummary, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, "\n"))
	if strings.Contains(text, "visual") || strings.Contains(text, "acceptance") {
		return true
	}
	for _, ref := range []string{item.HeadSHA, branch.HeadSHA, item.BranchID, branch.BranchName, item.ItemID, item.QueueID} {
		ref = strings.ToLower(strings.TrimSpace(ref))
		if ref == "" {
			continue
		}
		if strings.Contains(text, ref) {
			return true
		}
		if len(ref) >= 7 && strings.Contains(text, ref[:7]) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueVisualEvidenceDocNamesItem(doc WorkspaceDocRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
		return false
	}
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if !strings.Contains(text, "rhizome_visual_acceptance_v1") {
		return false
	}
	if !agentWorkPatchQueueVisualEvidenceHasHeadRef(text, item, branch) {
		return false
	}
	if !agentWorkPatchQueueVisualEvidenceHasCandidateRef(text, item, branch) {
		return false
	}
	return agentWorkPatchQueueVisualEvidenceHasVerdict(text)
}

func (s *Store) agentWorkPatchQueuePositiveValidationEvidenceDocExistsForItem(ctx context.Context, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) (bool, error) {
	summaries, err := s.ListWorkspaceDocs(ctx, workspaceID, false)
	if err != nil {
		return false, err
	}
	for _, summary := range summaries {
		if !agentWorkPatchQueueVisualEvidenceSummaryCouldMatch(summary, item, branch) {
			continue
		}
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, summary.DocKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return false, err
		}
		if doc.ArchivedAt != nil {
			continue
		}
		if agentWorkPatchQueuePositiveValidationEvidenceDocNamesItem(doc, item, branch) && agentWorkPatchQueueVisualEvidenceDocFreshForItem(doc, item) {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueuePositiveValidationEvidenceDocNamesItem(doc WorkspaceDocRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) ||
		projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
		return false
	}
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if projectPatchQueueSupersessionEvidenceMissingTargetRef(text, item, branch) != "" {
		return false
	}
	if projectPatchQueueSupersessionEvidenceHasExplicitNegativeVerdict(text) {
		return false
	}
	hasPositiveValidation := projectPatchQueueSupersessionEvidenceHasPositiveValidation(text)
	if projectPatchQueueSupersessionEvidenceRejectsProgress(text) && !projectPatchQueueSupersessionEvidenceClosesStaleBlocker(text, hasPositiveValidation) {
		return false
	}
	if len(projectPatchQueueSupersessionVisualAcceptanceMissingRequirements(text, item)) > 0 {
		return false
	}
	return hasPositiveValidation
}

func agentWorkPatchQueueVisualEvidenceHasHeadRef(text string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	headSHA := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)))
	if headSHA == "" {
		return true
	}
	for _, token := range agentWorkPatchQueueHeadTokenPattern.FindAllString(text, -1) {
		if agentWorkPatchQueueHeadsCompatible(token, headSHA) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueVisualEvidenceHasCandidateRef(text string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	for _, ref := range []string{item.BranchID, branch.BranchID, branch.BranchName, item.ItemID, item.QueueID} {
		ref = strings.ToLower(strings.TrimSpace(ref))
		if ref != "" && strings.Contains(text, ref) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueVisualEvidenceHasVerdict(text string) bool {
	return projectPatchQueueEvidenceHasStructuredFieldValue(text,
		[]string{"visual_verdict", "visualverdict", "validation_verdict", "validationverdict", "verdict"},
		[]string{"pass", "passed", "fail", "failed", "block", "blocked"}) ||
		projectPatchQueueEvidenceContainsAny(text, "severity: blocking", "blocking_findings", "blocking visual")
}

func agentWorkPatchQueueVisualEvidenceDocFreshForItem(doc WorkspaceDocRecord, item ProjectPatchQueueItemRecord) bool {
	referenceAt := strings.TrimSpace(item.DecidedAt)
	if referenceAt == "" {
		return true
	}
	docAt := strings.TrimSpace(doc.UpdatedAt)
	if docAt == "" {
		return true
	}
	docTime, docErr := time.Parse(time.RFC3339Nano, docAt)
	refTime, refErr := time.Parse(time.RFC3339Nano, referenceAt)
	if docErr == nil && refErr == nil {
		return docTime.After(refTime) || docTime.Equal(refTime)
	}
	return docAt >= referenceAt
}

func agentWorkPatchQueueGenericValidationSidecarCandidate(task WorkspaceTaskRecord) bool {
	if agentWorkPatchQueueDeterministicValidationOrReviewTask(task) {
		return false
	}
	if agentWorkPatchQueueEvidenceRefreshTask(task) {
		return false
	}
	if agentWorkPatchQueueRevisionFollowupTask(task) ||
		agentWorkPatchQueueSupersedeStewardshipTask(task) ||
		agentWorkPatchQueueClaimStewardshipTask(task) {
		return false
	}
	return agentWorkTaskLooksValidationScoped(task) || agentWorkTaskLooksReviewScoped(task) || agentWorkPatchQueueReviewTask(task)
}

func agentWorkPatchQueueDeterministicValidationTask(task WorkspaceTaskRecord) bool {
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	if strings.HasPrefix(taskID, "task-patchq-validation-") {
		return true
	}
	if !agentWorkPatchQueueTaskHasExplicitQueueItem(task) {
		return false
	}
	return agentWorkTaskLooksValidationScoped(task)
}

func agentWorkPatchQueueEvidenceRefreshTask(task WorkspaceTaskRecord) bool {
	requiredDocSchema := strings.ToLower(strings.TrimSpace(agentWorkTaskRequirementString(task, "required_doc_schema")))
	if requiredDocSchema == "" {
		return false
	}
	branchID := firstNonEmpty(
		agentWorkTaskRequirementString(task, "candidate_branch_id"),
		agentWorkTaskRequirementString(task, "branch_id"),
		agentWorkTaskRequirementString(task, "target_branch_id"),
	)
	headSHA := firstNonEmpty(
		agentWorkTaskRequirementString(task, "candidate_head_sha"),
		agentWorkTaskRequirementString(task, "head_sha"),
		agentWorkTaskRequirementString(task, "target_head_sha"),
	)
	if strings.TrimSpace(branchID) == "" || strings.TrimSpace(headSHA) == "" {
		return false
	}
	if requiredDocSchema == "rhizome_visual_acceptance_v1" {
		return true
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	return strings.Contains(requiredDocSchema, "visual") ||
		strings.Contains(requiredDocSchema, "acceptance") ||
		strings.Contains(text, "visual acceptance") ||
		strings.Contains(text, "browser evidence") ||
		strings.Contains(text, "browser/e2e")
}

func agentWorkQueueBoundValidationOrReviewTaskExistsForItem(tasks []WorkspaceTaskRecord, currentTaskID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == currentTaskID {
			continue
		}
		if agentWorkPatchQueueReviewReceiptTask(task) {
			continue
		}
		if !agentWorkPatchQueueDeterministicValidationOrReviewTask(task) && !agentWorkPatchQueueTaskHasExplicitQueueItem(task) {
			continue
		}
		if !(agentWorkTaskLooksValidationScoped(task) || agentWorkPatchQueueReviewTask(task)) {
			continue
		}
		if !agentWorkTaskMatchesPatchQueueArtifact(task, branch, item) || !agentWorkPatchQueueTaskHeadCompatible(task, item, branch) {
			continue
		}
		return true
	}
	return false
}

func agentWorkPatchQueueDeterministicValidationOrReviewTask(task WorkspaceTaskRecord) bool {
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	return strings.HasPrefix(taskID, "task-patchq-validation-") ||
		strings.HasPrefix(taskID, "task-patchq-review-") ||
		(strings.HasPrefix(taskID, "task-review-") && strings.Contains(taskID, "patchq") && strings.Contains(taskID, "patchitem"))
}

func agentWorkPatchQueueTaskHasExplicitQueueItem(task WorkspaceTaskRecord) bool {
	queueID, itemID, _ := agentWorkPatchQueueRefsFromTask(task)
	return strings.TrimSpace(queueID) != "" && strings.TrimSpace(itemID) != ""
}

var agentWorkPatchQueueHeadTokenPattern = regexp.MustCompile(`(?i)\b[0-9a-f]{7,40}\b`)

func agentWorkPatchQueueTaskHeadCompatible(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	itemHead := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)))
	if itemHead == "" {
		return true
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	tokens := agentWorkPatchQueueHeadTokenPattern.FindAllString(text, -1)
	if len(tokens) == 0 {
		return true
	}
	for _, token := range tokens {
		if agentWorkPatchQueueHeadsCompatible(token, itemHead) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueTaskHasCompatibleHeadToken(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	itemHead := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)))
	if itemHead == "" {
		return false
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	tokens := agentWorkPatchQueueHeadTokenPattern.FindAllString(text, -1)
	for _, token := range tokens {
		if agentWorkPatchQueueHeadsCompatible(token, itemHead) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueHeadsCompatible(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return true
	}
	if left == right {
		return true
	}
	return (len(left) >= 7 && strings.HasPrefix(right, left)) ||
		(len(right) >= 7 && strings.HasPrefix(left, right))
}

func (s *Store) agentWorkStalePatchQueueSidecarBehindRevisionFollowup(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkPatchQueueStaleSidecarCandidate(task) {
		return false, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, strings.TrimSpace(task.ProjectID))
	if err != nil {
		return false, err
	}
	branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		if id := strings.TrimSpace(branch.BranchID); id != "" {
			branches[id] = branch
		}
	}
	for _, item := range coordination.PatchQueueItems {
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		default:
			continue
		}
		if !agentWorkOpenPatchQueueRevisionFollowupExistsForItem(coordination.Tasks, item) {
			continue
		}
		branch := branches[strings.TrimSpace(item.BranchID)]
		if agentWorkPatchQueueSidecarRelatesToRevisionItem(task, item, branch) {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueueStaleSidecarCandidate(task WorkspaceTaskRecord) bool {
	if agentWorkPatchQueueEvidenceRefreshTask(task) {
		return false
	}
	if agentWorkPatchQueueRevisionFollowupTask(task) {
		return false
	}
	if agentWorkPatchQueueBacklogPromotionSidecarTask(task) {
		return true
	}
	if agentWorkTaskLooksActiveLanePublication(task) {
		return true
	}
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-side-effect-") {
		return true
	}
	if agentWorkTaskHasTag(task, "side-effect-classification", "side-effect-resolution", "operational-boundary", "abpc") {
		return true
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	if strings.Contains(text, "provenance") || strings.Contains(text, "publication") || strings.Contains(text, "publish candidate") || strings.Contains(text, "publish review-ready") {
		return strings.Contains(text, "patch-queue") || strings.Contains(text, "patch queue") || strings.Contains(text, "patch_queue")
	}
	return false
}

func agentWorkPatchQueueBacklogPromotionSidecarTask(task WorkspaceTaskRecord) bool {
	if !agentWorkTaskHasTag(task, "agent-backlog") {
		return false
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	if !strings.Contains(text, "patch_queue_vigilance") && !strings.Contains(text, "patch-queue-vigilance") {
		return false
	}
	return strings.Contains(text, "patch queue candidate needs queue stewardship") ||
		strings.Contains(text, "patch_queue_convergence_gap") ||
		strings.Contains(text, "patch_queue_review_owner_gap") ||
		strings.Contains(text, "missing:queue_stewardship") ||
		strings.Contains(text, "missing:review_owner")
}

func agentWorkTaskAllowsPatchQueueReviewPreemption(task WorkspaceTaskRecord) bool {
	if agentWorkPatchQueueReviewReceiptTask(task) || agentWorkPatchQueueRevisionFollowupTask(task) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) &&
		(projectLaneRequiresImplementationGate(task.ProjectLane) || task.RequiresProjectGate) {
		return false
	}
	if agentWorkTaskIsProactiveMetacognition(task) ||
		agentWorkPatchQueueStaleSidecarCandidate(task) ||
		agentWorkPatchQueueBacklogPromotionSidecarTask(task) ||
		agentWorkPatchQueueDecisionContinuationTask(task) ||
		agentWorkPatchQueueEvidenceTask(task) ||
		agentWorkPatchQueueSupersedeStewardshipTask(task) ||
		agentWorkPatchQueueClaimStewardshipTask(task) ||
		projectStrategicLeadCoordinationTask(task) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindCoordination) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(task.ProjectLane)) {
	case "coordination", "strategy", "review", "validation", "integration", "integrate", "integrator":
		return true
	default:
		return false
	}
}

func agentWorkOpenPatchQueueRevisionFollowupExistsForItem(tasks []WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if agentWorkPatchQueueRevisionFollowupMatchesItem(task, item) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueRevisionFollowupMatchesItem(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return false
	}
	if state := agentWorkPatchQueueRevisionFollowupDecisionState(task); state != "" {
		switch strings.ToUpper(state) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		default:
			return false
		}
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if queueID != "" && !strings.EqualFold(queueID, strings.TrimSpace(item.QueueID)) {
		return false
	}
	if itemID != "" && !strings.EqualFold(itemID, strings.TrimSpace(item.ItemID)) {
		return false
	}
	if branchID != "" && !strings.EqualFold(branchID, strings.TrimSpace(item.BranchID)) {
		return false
	}
	if queueID != "" || itemID != "" || branchID != "" {
		return branchID != "" || agentWorkPatchQueueTaskContainsRef(task, item.BranchID)
	}
	if strings.TrimSpace(item.BranchID) != "" && !agentWorkPatchQueueTaskContainsRef(task, item.BranchID) {
		return false
	}
	mentionsItem := agentWorkPatchQueueTaskContainsRef(task, item.ItemID) || agentWorkPatchQueueTaskContainsRef(task, item.QueueID)
	if strings.TrimSpace(item.HeadSHA) != "" && agentWorkPatchQueueTaskContainsRef(task, item.HeadSHA) {
		mentionsItem = true
	}
	return mentionsItem
}

func agentWorkPatchQueueRevisionFollowupDecisionState(task WorkspaceTaskRecord) string {
	if agentWorkTaskHasTag(task, "blocked") {
		return ProjectPatchQueueStateBlocked
	}
	if agentWorkTaskHasTag(task, "rejected") {
		return ProjectPatchQueueStateRejected
	}
	state := strings.ToLower(agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "state"))
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		return strings.ToUpper(strings.TrimSpace(state))
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	switch {
	case strings.Contains(text, "blocked patch queue item") || strings.Contains(text, "blocked patch-queue item") || strings.Contains(text, "terminal blocked"):
		return ProjectPatchQueueStateBlocked
	case strings.Contains(text, "rejected patch queue item") || strings.Contains(text, "rejected patch-queue item") || strings.Contains(text, "terminal rejected"):
		return ProjectPatchQueueStateRejected
	default:
		return ""
	}
}

func agentWorkPatchQueueSidecarRelatesToRevisionItem(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	if branchID := firstNonEmpty(
		agentWorkTaskRequirementString(task, "candidate_branch_id"),
		agentWorkTaskRequirementString(task, "branch_id"),
		agentWorkTaskRequirementString(task, "target_branch_id"),
		agentWorkTaskRequirementString(task, "branch"),
	); branchID != "" {
		if !strings.EqualFold(strings.TrimSpace(branchID), strings.TrimSpace(item.BranchID)) {
			return false
		}
		if headSHA := firstNonEmpty(
			agentWorkTaskRequirementString(task, "candidate_head_sha"),
			agentWorkTaskRequirementString(task, "head_sha"),
			agentWorkTaskRequirementString(task, "target_head_sha"),
		); headSHA != "" {
			return agentWorkPatchQueueHeadsCompatible(headSHA, firstNonEmpty(item.HeadSHA, branch.HeadSHA))
		}
		return true
	}
	if headSHA := firstNonEmpty(
		agentWorkTaskRequirementString(task, "candidate_head_sha"),
		agentWorkTaskRequirementString(task, "head_sha"),
		agentWorkTaskRequirementString(task, "target_head_sha"),
	); headSHA != "" {
		return agentWorkPatchQueueHeadsCompatible(headSHA, firstNonEmpty(item.HeadSHA, branch.HeadSHA))
	}
	if agentWorkPatchQueueTaskContainsRef(task, item.BranchID) ||
		agentWorkPatchQueueTaskContainsRef(task, item.ItemID) ||
		agentWorkPatchQueueTaskContainsRef(task, item.QueueID) ||
		agentWorkPatchQueueTaskContainsRef(task, item.HeadSHA) {
		return true
	}
	owner := strings.TrimSpace(branch.AgentID)
	if owner != "" && agentWorkPatchQueueTaskContainsRef(task, owner) {
		return true
	}
	return agentWorkTaskLooksActiveLanePublication(task) && agentWorkTaskHasTag(task, "patch-queue", "patch_queue")
}

func agentWorkTaskRequirementString(task WorkspaceTaskRecord, keys ...string) string {
	payload := agentWorkTaskRequirementsPayload(task)
	if len(payload) == 0 {
		return ""
	}
	for _, key := range keys {
		if value := strings.TrimSpace(agentWorkStringValue(payload[strings.TrimSpace(key)])); value != "" {
			return value
		}
	}
	return ""
}

func agentWorkPatchQueueTaskContainsRef(task WorkspaceTaskRecord, ref string) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	return agentWorkOwnerBoundTextContainsIdentifier(agentWorkPatchQueueTaskIdentityText(task), ref)
}

func agentWorkPatchQueueTaskIdentityText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.Description),
		strings.TrimSpace(task.TaskTemplate),
		strings.TrimSpace(task.ProjectLane),
		strings.TrimSpace(task.TaskRequirementsJSON),
		strings.Join(task.Tags, " "),
	}, "\n"))
}

func agentWorkPatchQueueItemSupersedes(candidate, stale ProjectPatchQueueItemRecord) bool {
	if strings.TrimSpace(candidate.SupersedesQueueID) == "" && strings.TrimSpace(candidate.SupersedesItemID) == "" {
		return false
	}
	if strings.TrimSpace(candidate.SupersedesQueueID) != "" && strings.TrimSpace(candidate.SupersedesQueueID) != strings.TrimSpace(stale.QueueID) {
		return false
	}
	if strings.TrimSpace(candidate.SupersedesItemID) != "" && strings.TrimSpace(candidate.SupersedesItemID) != strings.TrimSpace(stale.ItemID) {
		return false
	}
	return true
}

func agentWorkPatchQueueItemDecidedAfter(candidate, stale ProjectPatchQueueItemRecord) bool {
	candidateAt := strings.TrimSpace(firstNonEmpty(candidate.DecidedAt, candidate.UpdatedAt, candidate.CreatedAt))
	staleAt := strings.TrimSpace(firstNonEmpty(stale.DecidedAt, stale.UpdatedAt, stale.CreatedAt))
	if candidateAt == "" || staleAt == "" {
		return false
	}
	candidateTime, candidateErr := time.Parse(time.RFC3339Nano, candidateAt)
	staleTime, staleErr := time.Parse(time.RFC3339Nano, staleAt)
	if candidateErr == nil && staleErr == nil {
		return candidateTime.After(staleTime)
	}
	return candidateAt > staleAt
}

func (s *Store) agentWorkTerminalPatchQueueItemForTask(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (ProjectPatchQueueItemRecord, bool, error) {
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if branchID == "" {
		branchID = workspaceTaskPointerValue(task.ClaimBranchID)
	}
	if queueID == "" && itemID == "" && branchID == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(branchID),
	})
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		if branchID != "" && strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected, ProjectPatchQueueStateCanceled, ProjectPatchQueueStateIntegrated:
			return item, true, nil
		}
	}
	return ProjectPatchQueueItemRecord{}, false, nil
}

func (s *Store) agentWorkAcceptedPatchQueueCandidateHasLiveBranch(ctx context.Context, workspaceID, projectID string, item ProjectPatchQueueItemRecord) (bool, error) {
	_, live, err := s.agentWorkPatchQueueCandidateBranch(ctx, workspaceID, projectID, item)
	return live, err
}

func (s *Store) agentWorkPatchQueueCandidateBranch(ctx context.Context, workspaceID, projectID string, item ProjectPatchQueueItemRecord) (ProjectBranchRecord, bool, error) {
	branchID := strings.TrimSpace(item.BranchID)
	if branchID == "" {
		return ProjectBranchRecord{}, false, nil
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       strings.TrimSpace(projectID),
		RepoID:          strings.TrimSpace(item.RepoID),
		IncludeInactive: true,
	})
	if err != nil {
		return ProjectBranchRecord{}, false, err
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case ProjectBranchStatusAbandoned, ProjectBranchStatusArchived:
			return branch, false, nil
		default:
			return branch, strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)) != "", nil
		}
	}
	return ProjectBranchRecord{}, false, nil
}

func agentWorkPatchQueueItemsOverlap(left, right ProjectPatchQueueItemRecord) bool {
	leftPaths := agentWorkPatchQueueItemPathset(left)
	rightPaths := agentWorkPatchQueueItemPathset(right)
	if !writeScopesOverlap(leftPaths, rightPaths) {
		return false
	}
	for _, leftPath := range leftPaths {
		for _, rightPath := range rightPaths {
			if !writeScopePathOverlaps(leftPath, rightPath) {
				continue
			}
			if agentWorkPatchQueueCommonManifestPath(leftPath) && agentWorkPatchQueueCommonManifestPath(rightPath) {
				continue
			}
			return true
		}
	}
	return false
}

func agentWorkPatchQueueItemPathset(item ProjectPatchQueueItemRecord) []string {
	if len(item.Pathset) > 0 {
		return uniqueTrimmedStrings(item.Pathset)
	}
	_, paths, err := normalizeProjectPatchQueuePathsetJSON(item.PathsetJSON)
	if err != nil {
		return nil
	}
	return paths
}

func agentWorkPatchQueueCommonManifestPath(value string) bool {
	return writeScopeSharedSidecarPath(value)
}

func (s *Store) agentWorkStaleScaffoldTaskAfterAcceptedCandidate(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskLooksObsoleteScaffold(task) {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(task.ProjectID),
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated:
		default:
			continue
		}
		if !agentWorkTaskLinkedBeforePatchQueueItem(task, item) {
			continue
		}
		live, err := s.agentWorkAcceptedPatchQueueCandidateHasLiveBranch(ctx, workspaceID, task.ProjectID, item)
		if err != nil || !live {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func agentWorkTaskLinkedBeforePatchQueueItem(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	taskAt := strings.TrimSpace(firstNonEmpty(task.LinkedAt, task.UpdatedAt))
	itemAt := strings.TrimSpace(firstNonEmpty(item.DecidedAt, item.UpdatedAt, item.CreatedAt))
	if taskAt == "" || itemAt == "" {
		return false
	}
	taskTime, taskErr := time.Parse(time.RFC3339Nano, taskAt)
	itemTime, itemErr := time.Parse(time.RFC3339Nano, itemAt)
	if taskErr == nil && itemErr == nil {
		return taskTime.Before(itemTime)
	}
	return taskAt < itemAt
}

func agentWorkTaskLooksObsoleteScaffold(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") {
		return false
	}
	titleText := strings.ToLower(strings.Join([]string{
		task.Title,
		strings.Join(task.Tags, " "),
	}, " "))
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	if agentWorkTaskHasPostAcceptedImprovementIntent(titleText) {
		return false
	}
	for _, marker := range []string{
		"canonical web app",
		"app shell",
		"ui shell",
		"scaffold from seed",
		"initial scaffold",
		"seed scaffold",
		"canonical scaffold",
		"scaffold the canonical",
		"scaffold shell",
		"scaffold ui",
		"shell for tooling",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func agentWorkTaskHasPostAcceptedImprovementIntent(text string) bool {
	for _, marker := range []string{
		"export",
		"import",
		"accessibility",
		"qa",
		"preview",
		"persistence",
		"test",
		"tests",
		"harness",
		"polish",
		"settings",
		"panel",
		"pipeline",
		"convert",
		"conversion",
		"upload",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func agentWorkClaimRepairTextFieldValue(task WorkspaceTaskRecord, key string) string {
	return agentWorkTaskTextFieldValue([]string{task.Description}, key)
}

func agentWorkProjectClaimRepairBranchTerminalStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "MERGED", "ABANDONED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func agentWorkTaskHasTag(task WorkspaceTaskRecord, tags ...string) bool {
	for _, existing := range task.Tags {
		existing = strings.ToLower(strings.TrimSpace(existing))
		for _, tag := range tags {
			if existing == strings.ToLower(strings.TrimSpace(tag)) {
				return true
			}
		}
	}
	return false
}

func agentWorkTaskHasTagPrefix(task WorkspaceTaskRecord, prefixes ...string) bool {
	for _, existing := range task.Tags {
		existing = strings.ToLower(strings.TrimSpace(existing))
		for _, prefix := range prefixes {
			prefix = strings.ToLower(strings.TrimSpace(prefix))
			if prefix != "" && strings.HasPrefix(existing, prefix) {
				return true
			}
		}
	}
	return false
}

func projectClaimRepairTask(task WorkspaceTaskRecord) bool {
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-project-claim-repair-") {
		return true
	}
	return agentWorkTaskHasTag(task, "project-claim-repair")
}

func projectRoleScopeTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "coordination" && !agentWorkProjectLaneIsStrategy(lane) {
		return false
	}
	if projectRoleScopeTaskCanonicalShape(task) {
		return true
	}
	return projectRoleScopeAuthorityTransitionRequirements(task)
}

func projectRoleScopeTaskCanonicalShape(task WorkspaceTaskRecord) bool {
	if !strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-role-scope-") {
		return false
	}
	if !agentWorkTaskHasTag(task, "project-role-scope") {
		return false
	}
	title := strings.ToLower(strings.TrimSpace(task.Title))
	description := strings.ToLower(strings.TrimSpace(task.Description))
	return strings.HasPrefix(title, "resolve project role/scope request") ||
		strings.Contains(description, "# strategic lead role/scope request")
}

func projectRoleScopeAuthorityTransitionRequirements(task WorkspaceTaskRecord) bool {
	if !agentWorkTaskHasTag(task, "project-role-scope", "authority-transition", "authority_transition") {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskRequirementsJSON,
		task.Description,
		strings.Join(task.Tags, " "),
	}, "\n"))
	return strings.Contains(text, "project_role_scope_authority_transition.v1") ||
		strings.Contains(text, `"required_transition":"project_role_assign"`) ||
		strings.Contains(text, `"required_transition": "project_role_assign"`) ||
		strings.Contains(text, `"preferred_transition":"project_role_assign"`) ||
		strings.Contains(text, `"preferred_transition": "project_role_assign"`)
}

type agentWorkABPCRecoveryActionInfo struct {
	ABPCTaskClass        string
	ActionKind           string
	Decision             string
	PreferredTransition  string
	Summary              string
	OwnerAgentID         string
	TargetAgentID        string
	ClassificationTaskID string
}

func agentWorkTaskLooksABPCRecoveryAction(task WorkspaceTaskRecord) bool {
	_, ok := agentWorkABPCRecoveryAction(task)
	return ok
}

func agentWorkABPCRecoveryActionFromTaskPointer(task *WorkspaceTaskRecord) (agentWorkABPCRecoveryActionInfo, bool) {
	if task == nil {
		return agentWorkABPCRecoveryActionInfo{}, false
	}
	return agentWorkABPCRecoveryAction(*task)
}

func agentWorkABPCRecoveryAction(task WorkspaceTaskRecord) (agentWorkABPCRecoveryActionInfo, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(normalizeTaskRequirementsJSON(task.TaskRequirementsJSON)), &payload); err != nil {
		return agentWorkABPCRecoveryActionInfo{}, false
	}
	schema := strings.TrimSpace(agentWorkStringValue(payload["schema"]))
	abpcClass := strings.TrimSpace(agentWorkStringValue(payload["abpc_task_class"]))
	admissionKind := strings.TrimSpace(agentWorkStringValue(payload["admission_kind"]))
	if schema != "artifact_bound_side_effect_resolution_followup.v1" && admissionKind != "abpc_recovery_action" {
		return agentWorkABPCRecoveryActionInfo{}, false
	}
	actionKind := strings.TrimSpace(agentWorkStringValue(payload["action_kind"]))
	decision := strings.TrimSpace(agentWorkStringValue(payload["decision"]))
	preferred := strings.TrimSpace(agentWorkStringValue(payload["next_transition"]))
	switch actionKind {
	case "verify_bucket":
		preferred = "side_effect_resolve"
	case "reassign_bucket":
		preferred = "route_to_new_owner"
	case "revert_bucket":
		preferred = "remove_materialization"
	case "quarantine_bucket":
		preferred = "quarantine_materialization"
	case "split_foundation_bucket":
		preferred = "create_foundation_lane"
	}
	if preferred == "" {
		switch decision {
		case "request_verification":
			preferred = "side_effect_resolve"
		case "reassign":
			preferred = "route_to_new_owner"
		case "revert":
			preferred = "remove_materialization"
		case "quarantine":
			preferred = "quarantine_materialization"
		case "split_tension":
			preferred = "create_foundation_lane"
		default:
			preferred = "side_effect_resolve"
		}
	}
	summary := "ABPC side-effect recovery successor must execute the recorded recovery action; it is not product review-ready validation and must not wait for a review-ready branch."
	if actionKind != "" {
		summary = "ABPC side-effect recovery successor " + actionKind + " must execute before the blocked artifact lane can continue."
	}
	return agentWorkABPCRecoveryActionInfo{
		ABPCTaskClass:        abpcClass,
		ActionKind:           actionKind,
		Decision:             decision,
		PreferredTransition:  preferred,
		Summary:              summary,
		OwnerAgentID:         strings.TrimSpace(agentWorkStringValue(payload["owner_agent_id"])),
		TargetAgentID:        strings.TrimSpace(agentWorkStringValue(payload["target_agent_id"])),
		ClassificationTaskID: strings.TrimSpace(firstNonEmpty(agentWorkStringValue(payload["parent_classifier_task_id"]), agentWorkStringValue(payload["classification_task_id"]))),
	}, true
}

func agentWorkStringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func projectRepositoryRepairTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "" && lane != "coordination" && !agentWorkProjectLaneIsStrategy(lane) {
		return false
	}
	if agentWorkTaskHasTag(task, "project-repo-repair", "project-repository-repair", "repo-repair", "repository-repair") {
		return true
	}
	tagText := agentWorkTaskTagText(task)
	if (strings.Contains(tagText, "repo") || strings.Contains(tagText, "repository")) &&
		(strings.Contains(tagText, "strategic-lead") || strings.Contains(tagText, "strategic_lead") || strings.Contains(tagText, "lead-only")) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.TaskTemplate,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	if agentWorkTextContainsAny(text, []string{
		"project.repository.upsert",
		"project_repo_register",
		"project_repo_materialize",
		"repository mutation requires active strategic lead",
		"repo mutation requires active strategic lead",
	}) {
		return true
	}
	hasRepo := strings.Contains(text, "repo") || strings.Contains(text, "repository")
	hasLead := strings.Contains(text, "strategic lead") || strings.Contains(text, "strategic-lead") || strings.Contains(text, "active lead")
	if hasRepo && hasLead && strings.Contains(text, "canonical") &&
		(strings.Contains(text, "register") || strings.Contains(text, "materialize") || strings.Contains(text, "repair") || strings.Contains(text, "ready")) {
		return true
	}
	return false
}

func projectStrategicLeadCoordinationTask(task WorkspaceTaskRecord) bool {
	return projectClaimRepairTask(task) || projectRoleScopeTask(task) || projectRepositoryRepairTask(task)
}

func agentWorkPatchQueueReviewTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasTag(task, "patch_queue", "patch-queue") {
		return false
	}
	if agentWorkProjectLaneIsPatchQueueReview(task.ProjectLane) {
		return true
	}
	return agentWorkPatchQueueSupersedeStewardshipTask(task) || agentWorkPatchQueueClaimStewardshipTask(task)
}

func agentWorkPatchQueueReviewReceiptTask(task WorkspaceTaskRecord) bool {
	return strings.EqualFold(agentWorkTaskRequirementString(task, "patch_queue_task_kind"), "review_receipt")
}

func agentWorkPatchQueueDecisionContinuationTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")))
	switch kind {
	case "integration", "revision", "validation", "rebuild":
		return true
	default:
		return agentWorkTaskHasTag(task, "patch_queue", "patch-queue") && agentWorkTaskHasTag(task, "decision_continuation")
	}
}

func agentWorkPatchQueueSupersedeStewardshipTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasTag(task, "patch_queue", "patch-queue") {
		return false
	}
	if agentWorkTaskHasTag(task, "supersede", "queue-stewardship") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		agentWorkTaskSearchText(task),
		task.Description,
	}, " "))
	if !strings.Contains(text, "project_patch_queue_lifecycle") {
		return false
	}
	if strings.Contains(text, "supersede") || strings.Contains(text, "requeue") {
		return true
	}
	compact := agentWorkCompactActionText(text)
	return strings.Contains(compact, "actionsupersede") || strings.Contains(compact, "actionrequeue")
}

func agentWorkPatchQueueClaimStewardshipTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasTag(task, "patch_queue", "patch-queue") {
		return false
	}
	if agentWorkTaskHasTag(task, "claim-stewardship", "claimed-decision", "claim-expired") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		agentWorkTaskSearchText(task),
		task.Description,
	}, " "))
	return strings.Contains(text, "project_patch_queue_claim_stewardship_available") ||
		strings.Contains(text, "patch queue claim stewardship") ||
		strings.Contains(text, "claim stewardship")
}

func agentWorkCompactActionText(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	var b strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func agentWorkPatchQueueRevisionFollowupReferencesBranch(task WorkspaceTaskRecord, branchID string) bool {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return false
	}
	queueID, itemID, taskBranchID := agentWorkPatchQueueRefsFromTask(task)
	if taskBranchID != "" {
		return strings.EqualFold(strings.TrimSpace(taskBranchID), branchID)
	}
	if queueID != "" || itemID != "" {
		return false
	}
	return agentWorkPatchQueueTaskContainsRef(task, branchID) && agentWorkPatchQueueRevisionFollowupDecisionState(task) != ""
}

func agentWorkPatchQueueRevisionFollowupTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") {
		return false
	}
	text := agentWorkPatchQueueTaskIdentityText(task)
	hasPatchQueue := agentWorkTaskHasTag(task, "patch_queue", "patch-queue") ||
		strings.Contains(text, "patch queue") ||
		strings.Contains(text, "patch-queue") ||
		strings.Contains(text, "patch_queue") ||
		strings.Contains(text, "patchitem-")
	hasRevision := agentWorkTaskHasTag(task, "revision") ||
		strings.Contains(text, "revision") ||
		strings.Contains(text, "revise") ||
		strings.Contains(text, "revised candidate")
	return hasPatchQueue && hasRevision
}

func agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst bool, task WorkspaceTaskRecord) bool {
	// Trust-first softens generic lane/role fit only; patch queue actor checks remain hard.
	return trustFirst &&
		len(projectClaimRequiredRoleTypesForLane(task.ProjectLane)) > 0 &&
		!agentWorkPatchQueueDecisionContinuationTask(task)
}

func agentWorkABPCRecoveryActionBypassesProjectRoleLane(task WorkspaceTaskRecord) bool {
	recovery, ok := agentWorkABPCRecoveryAction(task)
	if !ok {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(recovery.ABPCTaskClass), "side_effect_verification") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(recovery.ActionKind), "verify_bucket")
}

func (s *Store) projectStrategicLeadRecoveryTaskForGate(ctx context.Context, workspaceID, agentID string, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, busyTasks map[string]struct{}, blockedTask WorkspaceTaskRecord, packet *AgentWorkPacket) (*WorkspaceTaskRecord, bool, error) {
	if packet == nil || packet.Gate == nil || !agentWorkStrategicLeadGateClosedPacket(packet) {
		return nil, false, nil
	}
	projectID := strings.TrimSpace(blockedTask.ProjectID)
	if projectID == "" {
		projectID = strings.TrimSpace(packet.ProjectID)
	}
	if projectID == "" {
		return nil, false, nil
	}
	if ok, err := s.agentMayRecoverProjectStrategicLead(ctx, workspaceID, agentID, projectID); err != nil || !ok {
		return nil, false, err
	}
	var pending *WorkspaceTaskRecord
	for i := range tasks {
		task := tasks[i]
		if strings.TrimSpace(task.ProjectID) != projectID || !agentWorkProjectLeadRecoveryTask(task) {
			continue
		}
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if _, busy := busyTasks[strings.TrimSpace(task.TaskID)]; busy {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
			continue
		}
		if isResumableClaimForAgent(task, agentID) {
			taskCopy := task
			return &taskCopy, true, nil
		}
		if task.Status == model.TaskStatusPending && claimAvailable(task.ClaimStatus) && pending == nil {
			taskCopy := task
			pending = &taskCopy
		}
	}
	if pending != nil {
		return pending, true, nil
	}
	return nil, false, nil
}

func agentWorkStrategicLeadGateClosedPacket(packet *AgentWorkPacket) bool {
	if packet == nil || packet.Gate == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_gate_closed") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(packet.Gate.GateType), "project_implementation_gate") {
		return false
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(packet.Gate.Summary)), "strategic_lead_active")
}

func (s *Store) agentMayRecoverProjectStrategicLead(ctx context.Context, workspaceID, agentID, projectID string) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	projectID = strings.TrimSpace(projectID)
	if agentID == "" || projectID == "" {
		return false, nil
	}
	if lead, ok, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID); err != nil {
		return false, err
	} else if ok {
		if strings.TrimSpace(lead.AgentID) == agentID {
			return true, nil
		}
		fresh, err := s.projectClaimRepairLeadAppearsFresh(ctx, workspaceID, lead.AgentID)
		if err != nil {
			return false, err
		}
		if fresh {
			return false, nil
		}
	}
	if canRecover, err := s.agentHasRecoverableProjectStrategicLeadRole(ctx, workspaceID, agentID, projectID); err != nil || canRecover {
		return canRecover, err
	}
	return s.agentCanBackstopProjectClaimRepair(ctx, workspaceID, agentID)
}

func (s *Store) agentHasRecoverableProjectStrategicLeadRole(ctx context.Context, workspaceID, agentID, projectID string) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	projectID = strings.TrimSpace(projectID)
	if agentID == "" || projectID == "" {
		return false, nil
	}
	roles, err := s.ListProjectRoles(ctx, workspaceID, projectID, true)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		if strings.TrimSpace(role.AgentID) != agentID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(role.RoleType), ProjectRoleStrategicLead) {
			continue
		}
		if projectRoleIsRecoverableStrategicLead(role) {
			return true, nil
		}
	}
	return false, nil
}

func projectRoleIsRecoverableStrategicLead(role ProjectRoleRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(role.RoleType), ProjectRoleStrategicLead) {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(role.Status)) {
	case ProjectRoleStatusActive, ProjectRoleStatusExpired:
		return true
	default:
		return false
	}
}

func agentWorkProjectLeadRecoveryTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if projectStrategicLeadCoordinationTask(task) || agentWorkTaskLooksAutonomousProjectRoot(task) {
		return true
	}
	return agentWorkProjectLaneIsStrategy(task.ProjectLane)
}

type projectClaimRepairSelectionMode string

const (
	projectClaimRepairSelectionDenied   projectClaimRepairSelectionMode = "denied"
	projectClaimRepairSelectionLead     projectClaimRepairSelectionMode = "lead"
	projectClaimRepairSelectionBackstop projectClaimRepairSelectionMode = "backstop"
)

func (s *Store) agentMaySelectProjectClaimRepairTask(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	mode, err := s.agentProjectClaimRepairSelectionMode(ctx, workspaceID, agentID, task)
	if err != nil {
		return false, err
	}
	return mode == projectClaimRepairSelectionLead || mode == projectClaimRepairSelectionBackstop, nil
}

func (s *Store) agentProjectClaimRepairSelectionMode(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (projectClaimRepairSelectionMode, error) {
	if !projectStrategicLeadCoordinationTask(task) {
		return projectClaimRepairSelectionLead, nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		return projectClaimRepairSelectionDenied, nil
	}
	lead, ok, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		return projectClaimRepairSelectionDenied, err
	}
	agentID = strings.TrimSpace(agentID)
	if ok && strings.TrimSpace(lead.AgentID) == agentID {
		return projectClaimRepairSelectionLead, nil
	}
	if ok {
		fresh, err := s.projectClaimRepairLeadAppearsFresh(ctx, workspaceID, lead.AgentID)
		if err != nil {
			return projectClaimRepairSelectionDenied, err
		}
		if fresh {
			return projectClaimRepairSelectionDenied, nil
		}
	}
	if canRecover, err := s.agentHasRecoverableProjectStrategicLeadRole(ctx, workspaceID, agentID, projectID); err != nil {
		return projectClaimRepairSelectionDenied, err
	} else if canRecover {
		return projectClaimRepairSelectionLead, nil
	}
	canBackstop, err := s.agentCanBackstopProjectClaimRepair(ctx, workspaceID, agentID)
	if err != nil {
		return projectClaimRepairSelectionDenied, err
	}
	if canBackstop {
		return projectClaimRepairSelectionBackstop, nil
	}
	return projectClaimRepairSelectionDenied, nil
}

func (s *Store) projectClaimRepairLeadAppearsFresh(ctx context.Context, workspaceID, leadAgentID string) (bool, error) {
	agent, err := s.GetAgent(ctx, workspaceID, leadAgentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, err
	}
	if agent.LastSeenAt == nil || strings.TrimSpace(*agent.LastSeenAt) == "" {
		return false, nil
	}
	seenAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*agent.LastSeenAt))
	if err != nil {
		return false, nil
	}
	return time.Since(seenAt) < projectClaimRepairLeadStaleAfter, nil
}

func (s *Store) agentCanBackstopProjectClaimRepair(ctx context.Context, workspaceID, agentID string) (bool, error) {
	agent, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, err
	}
	profile, err := s.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	if !agentProfileAllowsAutonomousExecution(profile) {
		return false, nil
	}
	switch agentProfileFreshSelectionMode(profile) {
	case "strategy", "synthesis":
		return true, nil
	default:
		return false, nil
	}
}

func (s *Store) agentMaySelectProjectPatchQueueReviewTask(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	if !agentWorkPatchQueueReviewTask(task) {
		return true, nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	agentID = strings.TrimSpace(agentID)
	if projectID == "" || agentID == "" {
		return false, nil
	}
	// Reviewer independence: never hand a review_receipt task to the agent that SUBMITTED the
	// item under review (the storage decide guard would reject its self-accept anyway, but
	// routing the review to the submitter just wastes a claim/lease cycle). Stewardship tasks
	// are exempt: a submitter may steward its own stale claim to cancel/withdraw.
	if excluded, err := s.agentWorkReviewTaskSubmitterExcluded(ctx, agentID, task); err != nil {
		return false, err
	} else if excluded {
		return false, nil
	}
	if ok, err := s.agentMaySelectProjectPatchQueueReviewReceiptState(ctx, workspaceID, task); err != nil || !ok {
		return ok, err
	}
	if ok, err := s.agentMaySelectProjectPatchQueueClaimStewardshipTask(ctx, workspaceID, agentID, task); err != nil || !ok {
		return ok, err
	}
	if ok, err := s.agentMaySelectProjectPatchQueueActiveClaim(ctx, workspaceID, agentID, task); err != nil || !ok {
		return ok, err
	}
	lead, ok, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		return false, err
	}
	if ok && strings.TrimSpace(lead.AgentID) == agentID {
		return true, nil
	}
	roles, err := s.ListProjectRoles(ctx, workspaceID, projectID, false)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		if strings.TrimSpace(role.AgentID) != agentID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(role.RoleType)) {
		case ProjectRoleReviewer, ProjectRoleIntegrator:
			return true, nil
		}
	}
	agent, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, err
	}
	profile, err := s.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	return agentWorkProfileOrRegisteredRoleAllowsPatchQueueReview(profile, agent), nil
}

// agentWorkReviewTaskSubmitterExcluded reports whether the task is a review_receipt task whose
// patch queue item was submitted by this same agent - in which case the agent must not select
// it (reviewer independence; see the self-accept guard in DecideProjectPatchQueueItemWithEvent).
func (s *Store) agentWorkReviewTaskSubmitterExcluded(ctx context.Context, agentID string, task WorkspaceTaskRecord) (bool, error) {
	if !strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "review_receipt") {
		return false, nil
	}
	queueID := strings.TrimSpace(agentWorkTaskRequirementString(task, "queue_id"))
	itemID := strings.TrimSpace(agentWorkTaskRequirementString(task, "item_id"))
	if queueID == "" || itemID == "" {
		return false, nil
	}
	var submittedBy string
	err := s.db.QueryRowContext(ctx,
		`SELECT submitted_by FROM project_patch_queue_items WHERE queue_id = ? AND item_id = ?`,
		queueID, itemID,
	).Scan(&submittedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query review task item submitter: %w", err)
	}
	submittedBy = strings.TrimSpace(submittedBy)
	return submittedBy != "" && submittedBy == strings.TrimSpace(agentID), nil
}

func (s *Store) agentHasExplicitProjectPatchQueueReviewCapability(ctx context.Context, workspaceID, agentID, projectID string) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	projectID = strings.TrimSpace(projectID)
	if agentID == "" || projectID == "" {
		return false, nil
	}
	roles, err := s.ListProjectRoles(ctx, workspaceID, projectID, false)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		if strings.TrimSpace(role.AgentID) != agentID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(role.RoleType)) {
		case ProjectRoleReviewer, ProjectRoleIntegrator:
			return true, nil
		}
	}
	agent, err := s.GetAgent(ctx, workspaceID, agentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return false, nil
		}
		return false, err
	}
	profile, err := s.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return false, err
	}
	return agentWorkProfileOrRegisteredRoleAllowsPatchQueueReview(profile, agent), nil
}

func (s *Store) agentMaySelectProjectPatchQueueReviewReceiptState(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (bool, error) {
	if !agentWorkPatchQueueReviewReceiptTask(task) {
		return true, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if strings.TrimSpace(queueID) == "" || strings.TrimSpace(itemID) == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(branchID),
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) != strings.TrimSpace(queueID) || strings.TrimSpace(item.ItemID) != strings.TrimSpace(itemID) {
			continue
		}
		return strings.EqualFold(strings.TrimSpace(item.State), ProjectPatchQueueStateProposed), nil
	}
	return false, nil
}

func (s *Store) agentMaySelectProjectPatchQueueActiveClaim(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if strings.TrimSpace(queueID) == "" || strings.TrimSpace(itemID) == "" {
		return true, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(branchID),
		State:       ProjectPatchQueueStateClaimed,
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) != strings.TrimSpace(queueID) || strings.TrimSpace(item.ItemID) != strings.TrimSpace(itemID) {
			continue
		}
		if projectPatchQueueClaimActiveAt(item, time.Now().UTC()) && strings.TrimSpace(item.ClaimedBy) != strings.TrimSpace(agentID) {
			return false, nil
		}
		return true, nil
	}
	return true, nil
}

func (s *Store) agentMaySelectProjectPatchQueueClaimStewardshipTask(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	if !agentWorkPatchQueueClaimStewardshipTask(task) {
		return true, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	if strings.TrimSpace(queueID) == "" || strings.TrimSpace(itemID) == "" {
		return false, nil
	}
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   strings.TrimSpace(task.ProjectID),
		BranchID:    strings.TrimSpace(branchID),
		State:       ProjectPatchQueueStateClaimed,
	})
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) != strings.TrimSpace(queueID) || strings.TrimSpace(item.ItemID) != strings.TrimSpace(itemID) {
			continue
		}
		if projectPatchQueueClaimActiveAt(item, time.Now().UTC()) && strings.TrimSpace(item.ClaimedBy) != strings.TrimSpace(agentID) {
			return false, nil
		}
		return true, nil
	}
	return true, nil
}

type agentWorkPatchQueueSupersedeCandidate struct {
	Project       ProjectRecord
	Item          ProjectPatchQueueItemRecord
	Branch        ProjectBranchRecord
	EvidenceDoc   WorkspaceDocRecord
	ReferenceTime time.Time
}

func (s *Store) agentWorkPatchQueueSupersedeAvailable(ctx context.Context, workspaceID, agentID string, tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string) (*AgentWorkPacket, bool, error) {
	projects, err := s.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, false, err
	}
	candidates := []agentWorkPatchQueueSupersedeCandidate{}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		roleProbe := WorkspaceTaskRecord{
			ProjectID:   projectID,
			TaskKind:    model.TaskKindExecution,
			ProjectLane: "review",
			Tags:        []string{"patch_queue"},
		}
		allowed, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, roleProbe)
		if err != nil {
			return nil, false, err
		}
		if !allowed {
			continue
		}
		coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			return nil, false, err
		}
		branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
		for _, branch := range coordination.Branches {
			branches[strings.TrimSpace(branch.BranchID)] = branch
		}
		for _, item := range coordination.PatchQueueItems {
			if strings.ToUpper(strings.TrimSpace(item.State)) != ProjectPatchQueueStateBlocked {
				continue
			}
			branch, ok := branches[strings.TrimSpace(item.BranchID)]
			if !ok {
				continue
			}
			if strings.ToUpper(strings.TrimSpace(branch.Status)) != ProjectBranchStatusReadyForReview {
				continue
			}
			if strings.TrimSpace(branch.HeadSHA) == "" || strings.TrimSpace(branch.HeadSHA) != strings.TrimSpace(item.HeadSHA) {
				continue
			}
			if agentWorkPatchQueueHasLiveSupersession(coordination.PatchQueueItems, item) {
				continue
			}
			doc, referenceTime, ok, err := s.agentWorkPatchQueueFreshSupersedeEvidenceDoc(ctx, workspaceID, item, branch, coordination.PatchQueueItems)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			if agentWorkOpenPatchQueueSupersedeTaskExists(tasks, dependencyBlocks, item, branch, doc.DocKey) {
				continue
			}
			candidates = append(candidates, agentWorkPatchQueueSupersedeCandidate{
				Project:       project,
				Item:          item,
				Branch:        branch,
				EvidenceDoc:   doc,
				ReferenceTime: referenceTime,
			})
		}
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		leftUpdated := strings.TrimSpace(left.EvidenceDoc.UpdatedAt)
		rightUpdated := strings.TrimSpace(right.EvidenceDoc.UpdatedAt)
		if leftUpdated != rightUpdated {
			return leftUpdated > rightUpdated
		}
		if left.Project.ProjectID != right.Project.ProjectID {
			return left.Project.ProjectID < right.Project.ProjectID
		}
		if left.Item.BranchID != right.Item.BranchID {
			return left.Item.BranchID < right.Item.BranchID
		}
		return left.Item.ItemID < right.Item.ItemID
	})
	packet := agentWorkPatchQueueSupersedeAvailablePacket(candidates[0])
	return packet, true, nil
}

func agentWorkPatchQueueHasLiveSupersession(items []ProjectPatchQueueItemRecord, oldItem ProjectPatchQueueItemRecord) bool {
	oldQueueID := strings.TrimSpace(oldItem.QueueID)
	oldItemID := strings.TrimSpace(oldItem.ItemID)
	oldBranchID := strings.TrimSpace(oldItem.BranchID)
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) == oldQueueID && strings.TrimSpace(item.ItemID) == oldItemID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed, ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated, ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		default:
			continue
		}
		if strings.TrimSpace(item.SupersedesQueueID) == oldQueueID && strings.TrimSpace(item.SupersedesItemID) == oldItemID {
			return true
		}
		if strings.TrimSpace(item.BranchID) == oldBranchID &&
			strings.TrimSpace(item.HeadSHA) == strings.TrimSpace(oldItem.HeadSHA) &&
			strings.TrimSpace(item.EvidenceDocKey) != "" &&
			agentWorkPatchQueueItemDecidedAfter(item, oldItem) {
			return true
		}
	}
	return false
}

func agentWorkOpenPatchQueueSupersedeTaskExists(tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, evidenceDocKey string) bool {
	queueID := strings.ToLower(strings.TrimSpace(item.QueueID))
	itemID := strings.ToLower(strings.TrimSpace(item.ItemID))
	branchID := strings.ToLower(strings.TrimSpace(item.BranchID))
	branchName := strings.ToLower(strings.TrimSpace(branch.BranchName))
	headSHA := strings.ToLower(strings.TrimSpace(item.HeadSHA))
	rawEvidenceDocKey := strings.TrimSpace(evidenceDocKey)
	evidenceDocKey = strings.ToLower(rawEvidenceDocKey)
	deterministicTaskID := strings.ToLower(agentWorkPatchQueueSupersedeTaskID(strings.TrimSpace(item.ProjectID), strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), strings.TrimSpace(item.BranchID), strings.TrimSpace(item.HeadSHA), rawEvidenceDocKey))
	for _, task := range tasks {
		// Parity with the materializer's idempotency key (SubmitTask dedups on this
		// deterministic ID regardless of status): an exact-ID match must count as "already
		// materialized" even when terminal (RESOLVED/FAILED/CANCELLED). Otherwise the route
		// re-emits a directive the agent can never recreate (SubmitTask dedups to the terminal
		// task), preempting the work.next frontier forever -- the frontier-preemption livelock.
		// Promoted ABOVE the terminal-skip; the fuzzy text-match below stays non-terminal-only so
		// unrelated terminal tasks are never over-suppressed. Fresh evidence carries a new doc key
		// -> new deterministic ID -> not matched here, so legitimate re-supersede is preserved.
		if deterministicTaskID != "" && strings.ToLower(strings.TrimSpace(task.TaskID)) == deterministicTaskID {
			return true
		}
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if agentWorkPatchQueueReviewReceiptTask(task) {
			continue
		}
		if strings.TrimSpace(task.ProjectID) != strings.TrimSpace(item.ProjectID) {
			continue
		}
		text := strings.ToLower(strings.Join(append([]string{task.TaskID, task.Title, task.Description}, task.Tags...), "\n"))
		if !(strings.Contains(text, "patch_queue") || strings.Contains(text, "patch-queue") || strings.Contains(text, "patch queue")) {
			continue
		}
		if !(strings.Contains(text, "supersede") || strings.Contains(text, "requeue") || strings.Contains(text, "steward")) {
			continue
		}
		if itemID != "" && !strings.Contains(text, itemID) {
			continue
		}
		if branchID != "" && !strings.Contains(text, branchID) {
			continue
		}
		if evidenceDocKey != "" && !strings.Contains(text, evidenceDocKey) {
			continue
		}
		if queueID != "" && strings.Contains(text, queueID) {
			return true
		}
		if headSHA != "" && strings.Contains(text, headSHA) {
			return true
		}
		if branchName != "" && strings.Contains(text, branchName) {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueSupersedeTaskID(projectID, queueID, itemID, branchID, headSHA, evidenceDocKey string) string {
	seed := strings.Join([]string{projectID, queueID, itemID, branchID, headSHA, evidenceDocKey}, "|")
	return agentWorkSanitizeRefSegment("task-patchq-supersede-" + agentWorkCompactRefSegment("project", projectID) + "-" + agentWorkShortHash(seed))
}

type agentWorkPatchQueueClaimCandidate struct {
	Project                 ProjectRecord
	Item                    ProjectPatchQueueItemRecord
	Branch                  ProjectBranchRecord
	ClaimActive             bool
	OperationBindingPresent bool
}

func (s *Store) agentWorkPatchQueueMissingReviewTaskAvailable(ctx context.Context, workspaceID, agentID string) (*AgentWorkPacket, bool, error) {
	projects, err := s.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, false, err
	}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		roleProbe := WorkspaceTaskRecord{
			ProjectID:   projectID,
			TaskKind:    model.TaskKindExecution,
			ProjectLane: "review",
			Tags:        []string{"patch_queue"},
		}
		allowed, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, roleProbe)
		if err != nil {
			return nil, false, err
		}
		if !allowed {
			continue
		}
		coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			return nil, false, err
		}
		branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
		for _, branch := range coordination.Branches {
			branches[strings.TrimSpace(branch.BranchID)] = branch
		}
		for _, item := range coordination.PatchQueueItems {
			if !projectPatchQueueReviewTaskRequired(item) || !item.MissingReviewTask {
				continue
			}
			branch := branches[strings.TrimSpace(item.BranchID)]
			return agentWorkPatchQueueMissingReviewTaskPacket(item, branch), true, nil
		}
	}
	return nil, false, nil
}

func agentWorkPatchQueueMissingReviewTaskPacket(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) *AgentWorkPacket {
	queueRef := strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID)
	summary := fmt.Sprintf("Patch queue item %s is %s but has no durable review task receipt; replay the submit/supersede path or run a reconcile repair before treating the queue as autonomous.", queueRef, strings.TrimSpace(item.State))
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_review_task_missing",
		ProjectID:           strings.TrimSpace(item.ProjectID),
		TaskKind:            string(model.TaskKindExecution),
		ProjectLane:         "review",
		RequiresProjectGate: false,
		CoordinationState:   "patch_queue_item_missing_review_task_receipt",
		PreferredTransition: "reconcile_patch_queue_review_task_receipt",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: uniqueTrimmedAgentWork([]string{item.ReviewDocKey, item.EvidenceDocKey, item.DecisionDocKey}),
			AnchorBranchIDs:  uniqueTrimmedAgentWork([]string{item.BranchID, branch.BranchID}),
		},
	}
}

func (s *Store) selectAgentWorkPatchQueueDecisionContinuationTask(ctx context.Context, workspaceID, agentID string, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}, trustFirst bool) (*WorkspaceTaskRecord, bool, error) {
	for _, task := range tasks {
		if !agentWorkPatchQueueDecisionContinuationTask(task) {
			continue
		}
		if isTerminalTaskStatus(task.Status) || task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
			continue
		}
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" {
			continue
		}
		if _, hasSession := agentSessionTasks[taskID]; hasSession {
			continue
		}
		if _, paused := pausedTasks[taskID]; paused {
			continue
		}
		if _, busy := busyTasks[taskID]; busy {
			continue
		}
		ownerBoundOwnerMatch := false
		if req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if ok {
			if req.RepairNeeded || strings.TrimSpace(req.RequiredAgentID) == "" {
				continue
			}
			if strings.TrimSpace(req.RequiredAgentID) != strings.TrimSpace(agentID) {
				continue
			}
			ownerBoundOwnerMatch = true
		}
		ownerID := strings.TrimSpace(task.OwnerUserID)
		if ownerID != "" && ownerID != "system" && ownerID != strings.TrimSpace(agentID) && !ownerBoundOwnerMatch {
			continue
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if superseded {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, taskID); len(blockers) > 0 {
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked && packet != nil {
			continue
		}
		if _, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil {
			return nil, false, err
		} else if blocked {
			continue
		}
		if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil {
			return nil, false, err
		} else if !ok {
			continue
		}
		if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, task); err != nil {
			return nil, false, err
		} else if !ok {
			continue
		}
		if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked && packet != nil {
			continue
		}
		if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil {
			return nil, false, err
		} else if blocked && packet != nil {
			continue
		}
		taskCopy := task
		return &taskCopy, true, nil
	}
	return nil, false, nil
}

func (s *Store) selectAgentWorkPendingPatchQueueReviewTask(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}, trustFirst bool) (*WorkspaceTaskRecord, bool, error) {
	candidates := []WorkspaceTaskRecord{}
	capabilityByProject := map[string]bool{}
	capabilityKnown := map[string]struct{}{}
	for _, task := range tasks {
		if !agentWorkPatchQueueReviewTask(task) || !agentWorkPatchQueueReviewReceiptTask(task) {
			continue
		}
		projectID := strings.TrimSpace(task.ProjectID)
		if projectID == "" {
			continue
		}
		if _, ok := capabilityKnown[projectID]; !ok {
			allowed, err := s.agentHasExplicitProjectPatchQueueReviewCapability(ctx, workspaceID, agentID, projectID)
			if err != nil {
				return nil, false, err
			}
			capabilityKnown[projectID] = struct{}{}
			capabilityByProject[projectID] = allowed
		}
		if !capabilityByProject[projectID] {
			continue
		}
		selectable, err := s.agentWorkProductLanePressureTaskSelectable(ctx, workspaceID, agentID, profile, task, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, trustFirst)
		if err != nil {
			return nil, false, err
		}
		if !selectable {
			continue
		}
		candidates = append(candidates, task)
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if priorityRankForAgentWork(left.Priority) != priorityRankForAgentWork(right.Priority) {
			return priorityRankForAgentWork(left.Priority) < priorityRankForAgentWork(right.Priority)
		}
		if strings.TrimSpace(left.UpdatedAt) != strings.TrimSpace(right.UpdatedAt) {
			return strings.TrimSpace(left.UpdatedAt) > strings.TrimSpace(right.UpdatedAt)
		}
		return strings.TrimSpace(left.TaskID) < strings.TrimSpace(right.TaskID)
	})
	taskCopy := candidates[0]
	return &taskCopy, true, nil
}

func (s *Store) selectAgentWorkProductLanePressureTask(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, tasks []WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}, trustFirst bool) (*WorkspaceTaskRecord, bool, error) {
	pressureCache := map[string]bool{}
	pressureKnown := map[string]struct{}{}
	candidates := []WorkspaceTaskRecord{}
	for _, task := range tasks {
		if !agentWorkProductLanePressureCandidate(task) {
			continue
		}
		projectID := strings.TrimSpace(task.ProjectID)
		pressure, err := s.agentWorkProjectHasProductLanePressure(ctx, workspaceID, projectID, pressureCache, pressureKnown)
		if err != nil {
			return nil, false, err
		}
		if !pressure {
			continue
		}
		selectable, err := s.agentWorkProductLanePressureTaskSelectable(ctx, workspaceID, agentID, profile, task, taskDependencyBlocks, agentSessionTasks, pausedTasks, busyTasks, trustFirst)
		if err != nil {
			return nil, false, err
		}
		if !selectable {
			continue
		}
		candidates = append(candidates, task)
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if agentWorkProductLanePressureRank(left) != agentWorkProductLanePressureRank(right) {
			return agentWorkProductLanePressureRank(left) < agentWorkProductLanePressureRank(right)
		}
		if priorityRankForAgentWork(left.Priority) != priorityRankForAgentWork(right.Priority) {
			return priorityRankForAgentWork(left.Priority) < priorityRankForAgentWork(right.Priority)
		}
		if strings.TrimSpace(left.UpdatedAt) != strings.TrimSpace(right.UpdatedAt) {
			return strings.TrimSpace(left.UpdatedAt) > strings.TrimSpace(right.UpdatedAt)
		}
		return strings.TrimSpace(left.TaskID) < strings.TrimSpace(right.TaskID)
	})
	taskCopy := candidates[0]
	return &taskCopy, true, nil
}

func (s *Store) agentWorkProjectHasProductLanePressure(ctx context.Context, workspaceID, projectID string, cache map[string]bool, known map[string]struct{}) (bool, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return false, nil
	}
	if _, ok := known[projectID]; ok {
		return cache[projectID], nil
	}
	known[projectID] = struct{}{}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return false, err
	}
	for _, item := range coordination.PatchQueueItems {
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
			cache[projectID] = true
			return true, nil
		case ProjectPatchQueueStateProposed:
			if agentWorkPatchQueueItemHasReadyReviewBranch(item, coordination.Branches) {
				cache[projectID] = true
				return true, nil
			}
		}
	}
	cache[projectID] = false
	return false, nil
}

func agentWorkPatchQueueItemHasReadyReviewBranch(item ProjectPatchQueueItemRecord, branches []ProjectBranchRecord) bool {
	if strings.ToUpper(strings.TrimSpace(item.State)) != ProjectPatchQueueStateProposed {
		return false
	}
	if strings.TrimSpace(item.DecisionDocKey) != "" || strings.TrimSpace(item.DecisionSummary) != "" || strings.TrimSpace(item.DecidedAt) != "" {
		return false
	}
	branchID := strings.TrimSpace(item.BranchID)
	headSHA := strings.TrimSpace(item.HeadSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		return strings.ToUpper(strings.TrimSpace(branch.Status)) == ProjectBranchStatusReadyForReview &&
			strings.TrimSpace(branch.HeadSHA) == headSHA &&
			strings.TrimSpace(branch.ReviewDocKey) != ""
	}
	return false
}

func (s *Store) agentWorkProductLanePressureProjects(ctx context.Context, workspaceID string, tasks []WorkspaceTaskRecord) (map[string]struct{}, error) {
	projectsWithProductWork := map[string]struct{}{}
	for _, task := range tasks {
		if !agentWorkProductLanePressureCandidate(task) || isTerminalTaskStatus(task.Status) {
			continue
		}
		projectID := strings.TrimSpace(task.ProjectID)
		if projectID == "" {
			continue
		}
		projectsWithProductWork[projectID] = struct{}{}
	}
	if len(projectsWithProductWork) == 0 {
		return nil, nil
	}

	pressureProjects := map[string]struct{}{}
	pressureCache := map[string]bool{}
	pressureKnown := map[string]struct{}{}
	for projectID := range projectsWithProductWork {
		pressure, err := s.agentWorkProjectHasProductLanePressure(ctx, workspaceID, projectID, pressureCache, pressureKnown)
		if err != nil {
			return nil, err
		}
		if pressure {
			pressureProjects[projectID] = struct{}{}
		}
	}
	if len(pressureProjects) == 0 {
		return nil, nil
	}
	return pressureProjects, nil
}

func agentWorkTaskProjectUnderProductLanePressure(task WorkspaceTaskRecord, pressureProjects map[string]struct{}) bool {
	if len(pressureProjects) == 0 {
		return false
	}
	_, ok := pressureProjects[strings.TrimSpace(task.ProjectID)]
	return ok
}

func agentWorkTaskProjectByID(tasks []WorkspaceTaskRecord) map[string]string {
	projectByTask := make(map[string]string, len(tasks))
	for _, task := range tasks {
		taskID := strings.TrimSpace(task.TaskID)
		projectID := strings.TrimSpace(task.ProjectID)
		if taskID == "" || projectID == "" {
			continue
		}
		projectByTask[taskID] = projectID
	}
	return projectByTask
}

func agentWorkTaskBlockedByProductLanePressure(task WorkspaceTaskRecord, pressureProjects map[string]struct{}, taskProjectByID map[string]string) bool {
	underPressure := agentWorkTaskProjectUnderProductLanePressure(task, pressureProjects) ||
		agentWorkProjectlessTaskReferencesProductLanePressure(task, pressureProjects, taskProjectByID)
	if !underPressure {
		return false
	}
	if agentWorkProductLanePressureCandidate(task) {
		return false
	}
	if agentWorkPatchQueueCoordinationRepairTask(task) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindCoordination) {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	return lane == "coordination" || lane == "strategy"
}

func agentWorkPressureVisibleBlockedProductFrontierCandidate(candidate AgentWorkTaskFrontierCandidate, pressureProjects map[string]struct{}) bool {
	return candidate.Blocked &&
		agentWorkProductLanePressureCandidate(candidate.Task) &&
		agentWorkTaskProjectUnderProductLanePressure(candidate.Task, pressureProjects)
}

func agentWorkProjectlessTaskReferencesProductLanePressure(task WorkspaceTaskRecord, pressureProjects map[string]struct{}, taskProjectByID map[string]string) bool {
	if len(pressureProjects) == 0 || strings.TrimSpace(task.ProjectID) != "" || !agentWorkTaskIsCoordinationOrStrategy(task) {
		return false
	}
	for _, taskID := range agentWorkTaskPressureReferenceTaskIDs(task) {
		projectID := strings.TrimSpace(taskProjectByID[strings.TrimSpace(taskID)])
		if projectID == "" {
			continue
		}
		if _, ok := pressureProjects[projectID]; ok {
			return true
		}
	}
	return false
}

func agentWorkTaskIsCoordinationOrStrategy(task WorkspaceTaskRecord) bool {
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindCoordination) {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	return lane == "coordination" || lane == "strategy"
}

func agentWorkTaskPressureReferenceTaskIDs(task WorkspaceTaskRecord) []string {
	refs := []string{
		agentWorkTaskRequirementString(task, "blocked_task_id"),
		agentWorkTaskRequirementString(task, "target_task_id"),
		agentWorkTaskRequirementString(task, "dependency_task_id"),
		agentWorkTaskRequirementString(task, "classification_task_id"),
		agentWorkTaskRequirementString(task, "parent_classifier_task_id"),
	}
	refs = append(refs, agentWorkTaskRequirementsStringSlice(task,
		"advisory_dependency_task_ids",
		"blocked_task_ids",
		"target_task_ids",
		"dependency_task_ids",
	)...)
	refs = append(refs, extractHydrationTaskIDs(agentWorkTaskPressureReferenceText(task))...)
	return uniqueTrimmedAgentWork(refs)
}

func agentWorkTaskPressureReferenceText(task WorkspaceTaskRecord) string {
	parts := []string{
		task.Title,
		task.Description,
		task.TaskRequirementsJSON,
		strings.Join(task.Tags, " "),
		strings.Join(task.WriteScopeHints, " "),
	}
	return strings.Join(parts, "\n")
}

func agentWorkPatchQueueCoordinationRepairTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	// Product pressure may be bypassed by typed follow-through carriers, not
	// by free-form coordination tasks that merely mention patch queue repair.
	return projectClaimRepairTask(task) ||
		projectRoleScopeTask(task) ||
		projectRepositoryRepairTask(task) ||
		agentWorkPatchQueueDecisionContinuationTask(task) ||
		agentWorkPatchQueueRevisionFollowupTask(task) ||
		agentWorkPatchQueueAmbientRepairTask(task) ||
		agentWorkPatchQueueEvidenceTask(task) ||
		agentWorkPatchQueueSupersedeStewardshipTask(task) ||
		agentWorkPatchQueueClaimStewardshipTask(task) ||
		agentWorkPatchQueueReviewTask(task)
}

func agentWorkPatchQueueAmbientRepairTask(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !agentWorkTaskHasTag(task, "ambient-repair") {
		return false
	}
	return agentWorkTaskHasTag(task, "patch_queue", "patch-queue") &&
		agentWorkTaskHasTag(task, "revision")
}

func productLanePressureCoordinationBlockSummary() string {
	return "Project has terminal patch queue pressure and pending product-lane work; suppress non-product coordination until execution, review, or integration work advances."
}

func (s *Store) agentWorkProductLanePressureTaskSelectable(ctx context.Context, workspaceID, agentID string, profile AgentProfileRecord, task WorkspaceTaskRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}, trustFirst bool) (bool, error) {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" || isTerminalTaskStatus(task.Status) || task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
		return false, nil
	}
	if _, hasSession := agentSessionTasks[taskID]; hasSession {
		return false, nil
	}
	if _, paused := pausedTasks[taskID]; paused {
		return false, nil
	}
	if _, busy := busyTasks[taskID]; busy {
		return false, nil
	}
	if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil || superseded {
		return false, err
	}
	if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, taskID); len(blockers) > 0 {
		return false, nil
	}
	if _, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil || blocked {
		return false, err
	}
	if _, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil || blocked {
		return false, err
	}
	if !agentWorkPatchQueueReviewTask(task) && !agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst) {
		bypass, err := s.agentWorkMayBypassFreshProfileGate(ctx, workspaceID, agentID, task)
		if err != nil {
			return false, err
		}
		if !bypass {
			return false, nil
		}
	}
	if !agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, task) {
		if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil || !ok {
			return false, err
		}
	}
	if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, task); err != nil || !ok {
		return false, err
	}
	if _, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil || blocked {
		return false, err
	}
	if _, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil || blocked {
		return false, err
	}
	if _, targeted, err := s.projectImplementationFreshClaimRequiresTargetedSwitch(ctx, workspaceID, agentID, task); err != nil || (targeted && !trustFirst) {
		return false, err
	}
	return true, nil
}

func agentWorkProductLanePressureCandidate(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if projectStrategicLeadCoordinationTask(task) ||
		projectClaimRepairTask(task) ||
		projectRoleScopeTask(task) ||
		projectRepositoryRepairTask(task) ||
		agentWorkTaskLooksActiveLanePublication(task) ||
		agentWorkPatchQueueBacklogPromotionSidecarTask(task) {
		return false
	}
	if agentWorkPatchQueueRevisionFollowupTask(task) ||
		agentWorkPatchQueueEvidenceTask(task) ||
		agentWorkPatchQueueSupersedeStewardshipTask(task) ||
		agentWorkPatchQueueReviewTask(task) {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane == "" {
		return projectTaskRequiresImplementationGate(task)
	}
	if projectLaneRequiresImplementationGate(lane) || agentWorkProjectLaneIsReview(lane) || agentWorkProjectLaneIsValidation(lane) {
		return true
	}
	switch lane {
	case "integration", "integrate", "integrator", "release", "merge", "ship":
		return true
	default:
		return false
	}
}

func agentWorkProductLanePressureRank(task WorkspaceTaskRecord) int {
	switch {
	case agentWorkPatchQueueRevisionFollowupTask(task):
		return 0
	case agentWorkPatchQueueSupersedeStewardshipTask(task):
		return 1
	case agentWorkPatchQueueEvidenceTask(task) || agentWorkPatchQueueReviewTask(task):
		return 2
	case projectLaneRequiresImplementationGate(task.ProjectLane):
		return 3
	case agentWorkProjectLaneIsValidation(task.ProjectLane) || agentWorkProjectLaneIsReview(task.ProjectLane):
		return 4
	default:
		return 5
	}
}

func (s *Store) agentWorkPatchQueueDecisionContinuationAvailable(ctx context.Context, workspaceID, agentID string) (*AgentWorkPacket, bool, error) {
	projects, err := s.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, false, err
	}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		roleProbe := WorkspaceTaskRecord{
			ProjectID:   projectID,
			TaskKind:    model.TaskKindExecution,
			ProjectLane: "review",
			Tags:        []string{"patch_queue", "review"},
		}
		allowed, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, roleProbe)
		if err != nil {
			return nil, false, err
		}
		if !allowed {
			continue
		}
		continuations, err := s.ListProjectPatchQueueDecisionContinuations(ctx, ProjectPatchQueueDecisionContinuationFilter{
			WorkspaceID: workspaceID,
			ProjectID:   projectID,
			State:       "PENDING",
		})
		if err != nil {
			return nil, false, err
		}
		for _, record := range continuations {
			if strings.TrimSpace(record.ContinuationTaskID) == "" {
				continue
			}
			return agentWorkPatchQueueDecisionContinuationPacket(record), true, nil
		}
	}
	return nil, false, nil
}

func agentWorkPatchQueueDecisionContinuationPacket(record ProjectPatchQueueDecisionContinuationRecord) *AgentWorkPacket {
	queueRef := strings.TrimSpace(record.QueueID) + "/" + strings.TrimSpace(record.ItemID)
	kind := strings.TrimSpace(record.FollowupKind)
	if kind == "" {
		kind = "continuation"
	}
	summary := fmt.Sprintf("Patch queue decision %s for %s has durable pending continuation outbox %s; consume it exactly once into visible %s work before treating the decision as replay-complete.", strings.TrimSpace(record.Decision), queueRef, strings.TrimSpace(record.OutboxID), kind)
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_decision_continuation_pending",
		ProjectID:           strings.TrimSpace(record.ProjectID),
		TaskKind:            string(model.TaskKindExecution),
		ProjectLane:         projectPatchQueueDecisionContinuationProjectLane(record),
		RequiresProjectGate: false,
		CoordinationState:   "patch_queue_decision_continuation_pending",
		PreferredTransition: "project_patch_queue_lifecycle action=consume_continuation",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: uniqueTrimmedAgentWork([]string{record.DecisionDocKey}),
			AnchorBranchIDs:  uniqueTrimmedAgentWork([]string{record.BranchID}),
		},
	}
}

func (s *Store) agentWorkPatchQueueClaimStewardshipAvailable(ctx context.Context, workspaceID, agentID string, tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string, authority WorkspaceTimeAuthority) (*AgentWorkPacket, bool, error) {
	projects, err := s.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, false, err
	}
	referenceTime := time.Now().UTC()
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(authority.ReferenceAt)); err == nil {
		referenceTime = parsed
	}
	candidates := []agentWorkPatchQueueClaimCandidate{}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		roleProbe := WorkspaceTaskRecord{
			ProjectID:   projectID,
			TaskKind:    model.TaskKindExecution,
			ProjectLane: "review",
			Tags:        []string{"patch_queue"},
		}
		allowed, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, roleProbe)
		if err != nil {
			return nil, false, err
		}
		if !allowed {
			continue
		}
		coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			return nil, false, err
		}
		branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
		for _, branch := range coordination.Branches {
			branches[strings.TrimSpace(branch.BranchID)] = branch
		}
		for _, item := range coordination.PatchQueueItems {
			if strings.ToUpper(strings.TrimSpace(item.State)) != ProjectPatchQueueStateClaimed {
				continue
			}
			claimedBy := strings.TrimSpace(item.ClaimedBy)
			if claimedBy == "" {
				continue
			}
			claimActive := projectPatchQueueClaimActiveAt(item, referenceTime)
			if claimActive && claimedBy != agentID {
				continue
			}
			if agentWorkOpenPatchQueueClaimStewardshipTaskExists(tasks, dependencyBlocks, item) {
				continue
			}
			candidates = append(candidates, agentWorkPatchQueueClaimCandidate{
				Project:                 project,
				Item:                    item,
				Branch:                  branches[strings.TrimSpace(item.BranchID)],
				ClaimActive:             claimActive,
				OperationBindingPresent: projectPatchQueueOperationBindingEvidencePresent(item),
			})
		}
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.ClaimActive != right.ClaimActive {
			return !left.ClaimActive
		}
		if strings.TrimSpace(left.Item.ClaimExpiresAt) != strings.TrimSpace(right.Item.ClaimExpiresAt) {
			return strings.TrimSpace(left.Item.ClaimExpiresAt) < strings.TrimSpace(right.Item.ClaimExpiresAt)
		}
		if left.Project.ProjectID != right.Project.ProjectID {
			return left.Project.ProjectID < right.Project.ProjectID
		}
		if left.Item.BranchID != right.Item.BranchID {
			return left.Item.BranchID < right.Item.BranchID
		}
		return left.Item.ItemID < right.Item.ItemID
	})
	return agentWorkPatchQueueClaimStewardshipPacket(candidates[0]), true, nil
}

type agentWorkPatchQueueSubmitHandoffCandidate struct {
	Project ProjectRecord
	Branch  ProjectBranchRecord
}

func agentWorkTaskAllowsPatchQueueSubmitHandoffPreemption(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	kind := strings.TrimSpace(task.TaskKind)
	if kind != "" &&
		!strings.EqualFold(kind, model.TaskKindExecution) &&
		!strings.EqualFold(kind, model.TaskKindCoordination) {
		return false
	}
	if agentWorkTaskHasOwnerBoundSignal(task) || agentWorkTaskLooksActiveLanePublication(task) {
		return false
	}
	return projectTaskRequiresImplementationGate(task) && projectLaneRequiresImplementationGate(task.ProjectLane)
}

func (s *Store) agentWorkPatchQueueSubmitHandoffAvailable(ctx context.Context, workspaceID, agentID string, tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string) (*AgentWorkPacket, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, false, nil
	}
	if agentWorkOpenPatchQueueSubmitHandoffTaskExistsForAgent(tasks, agentID) {
		return nil, false, nil
	}
	if exists, err := s.agentWorkOpenOwnerBoundTaskExistsForAgent(ctx, workspaceID, tasks, agentID); err != nil {
		return nil, false, err
	} else if exists {
		return nil, false, nil
	}
	projects, err := s.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, false, err
	}
	candidates := []agentWorkPatchQueueSubmitHandoffCandidate{}
	for _, project := range projects {
		projectID := strings.TrimSpace(project.ProjectID)
		if projectID == "" {
			continue
		}
		coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			return nil, false, err
		}
		for _, branch := range coordination.Branches {
			if strings.TrimSpace(branch.AgentID) != agentID {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(branch.Status), ProjectBranchStatusReadyForReview) {
				continue
			}
			if strings.TrimSpace(branch.HeadSHA) == "" || strings.TrimSpace(branch.ReviewDocKey) == "" {
				continue
			}
			if agentWorkPatchQueueItemExistsForBranchHead(coordination.PatchQueueItems, branch.BranchID, branch.HeadSHA) {
				continue
			}
			if agentWorkOpenPatchQueueSubmitHandoffTaskExists(tasks, dependencyBlocks, branch) {
				continue
			}
			// A durable owner-submit BLOCK receipt for this branch suppresses re-offering
			// the handoff even after the stale handoff task itself was receipt-cancelled:
			// the submit was already attempted and durably blocked, so the recovery path
			// is revision/repair, not an immediate re-prompt loop.
			if blocked, err := s.agentWorkOwnerSubmitBlockReceiptExistsForBranch(ctx, workspaceID, tasks, branch.BranchID); err != nil {
				return nil, false, err
			} else if blocked {
				continue
			}
			candidates = append(candidates, agentWorkPatchQueueSubmitHandoffCandidate{
				Project: project,
				Branch:  branch,
			})
		}
	}
	if len(candidates) == 0 {
		return nil, false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		leftUpdated := strings.TrimSpace(left.Branch.UpdatedAt)
		rightUpdated := strings.TrimSpace(right.Branch.UpdatedAt)
		if leftUpdated != rightUpdated {
			return leftUpdated > rightUpdated
		}
		if left.Project.ProjectID != right.Project.ProjectID {
			return left.Project.ProjectID < right.Project.ProjectID
		}
		return left.Branch.BranchID < right.Branch.BranchID
	})
	return agentWorkPatchQueueSubmitHandoffPacket(candidates[0]), true, nil
}

// agentWorkOwnerSubmitBlockReceiptExistsForBranch reports whether any owner-submit
// handoff task bound to the branch carries a durable task.blocked runtime event - the
// same durable signal the receipt sweep cancels stale handoffs on
// (owner_submit_blocked_handoff_receipt). Used to keep a receipt-cancelled handoff from
// being immediately re-offered for the same branch.
func (s *Store) agentWorkOwnerSubmitBlockReceiptExistsForBranch(ctx context.Context, workspaceID string, tasks []WorkspaceTaskRecord, branchID string) (bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return false, nil
	}
	for _, task := range tasks {
		if !agentWorkTaskHasOwnerBoundSignal(task) {
			continue
		}
		req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
		if err != nil {
			return false, err
		}
		if !ok || !strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") || strings.TrimSpace(req.BranchID) != branchID {
			continue
		}
		var count int
		if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND task_id = ?
   AND event_type = 'task.blocked'
   AND entity_type = 'task'
   AND entity_id = ?`,
			strings.TrimSpace(workspaceID),
			strings.TrimSpace(task.TaskID),
			strings.TrimSpace(task.TaskID),
		).Scan(&count); err != nil {
			return false, fmt.Errorf("check owner-submit block receipt for branch: %w", err)
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) agentWorkOpenOwnerBoundTaskExistsForAgent(ctx context.Context, workspaceID string, tasks []WorkspaceTaskRecord, agentID string) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false, nil
	}
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) || !agentWorkTaskHasOwnerBoundSignal(task) {
			continue
		}
		req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
		if err != nil {
			return false, err
		}
		if !ok || req.RepairNeeded {
			continue
		}
		if strings.TrimSpace(req.RequiredAgentID) == agentID {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueueItemExistsForBranchHead(items []ProjectPatchQueueItemRecord, branchID, headSHA string) bool {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) == branchID && strings.TrimSpace(item.HeadSHA) == headSHA {
			return true
		}
	}
	return false
}

func agentWorkOpenPatchQueueSubmitHandoffTaskExists(tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string, branch ProjectBranchRecord) bool {
	branchID := strings.ToLower(strings.TrimSpace(branch.BranchID))
	headSHA := strings.ToLower(strings.TrimSpace(branch.HeadSHA))
	reviewDocKey := strings.ToLower(strings.TrimSpace(branch.ReviewDocKey))
	deterministicTaskID := strings.ToLower(agentWorkPatchQueueSubmitHandoffTaskID(strings.TrimSpace(branch.ProjectID), strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.HeadSHA), strings.TrimSpace(branch.ReviewDocKey)))
	for _, task := range tasks {
		// Parity with the materializer's idempotency key (see agentWorkOpenPatchQueueSupersedeTaskExists):
		// an exact deterministic-ID match must count as "already materialized" even when terminal, so a
		// RESOLVED submit-handoff task cannot be re-emitted as a directive SubmitTask just dedups to,
		// preempting the work.next frontier. A new head/review-doc -> new deterministic ID -> still fires.
		if deterministicTaskID != "" && strings.ToLower(strings.TrimSpace(task.TaskID)) == deterministicTaskID {
			return true
		}
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if strings.TrimSpace(task.ProjectID) != strings.TrimSpace(branch.ProjectID) {
			continue
		}
		if blockers := unresolvedAgentWorkDependencyIDs(dependencyBlocks, task.TaskID); len(blockers) > 0 {
			continue
		}
		text := strings.ToLower(strings.Join(append([]string{task.TaskID, task.Title, task.Description}, task.Tags...), "\n"))
		if !(strings.Contains(text, "owner-bound-kind:patch_queue_submit") ||
			strings.Contains(text, "owner_bound_kind:patch_queue_submit") ||
			strings.Contains(text, "owner-submit") ||
			strings.Contains(text, "owner submit")) {
			continue
		}
		if branchID != "" && !strings.Contains(text, branchID) {
			continue
		}
		if headSHA != "" && strings.Contains(text, headSHA) {
			return true
		}
		if reviewDocKey != "" && strings.Contains(text, reviewDocKey) {
			return true
		}
		return true
	}
	return false
}

func agentWorkOpenPatchQueueSubmitHandoffTaskExistsForAgent(tasks []WorkspaceTaskRecord, agentID string) bool {
	agentID = strings.ToLower(strings.TrimSpace(agentID))
	if agentID == "" {
		return false
	}
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		text := strings.ToLower(strings.Join(append([]string{task.TaskID, task.Title, task.TaskTemplate}, task.Tags...), "\n"))
		if !(strings.Contains(text, "owner-bound-kind:patch_queue_submit") ||
			strings.Contains(text, "owner_bound_kind:patch_queue_submit") ||
			strings.Contains(text, "owner-submit") ||
			strings.Contains(text, "owner submit")) {
			continue
		}
		required := strings.ToLower(firstNonEmpty(
			agentWorkTaskTagValue(task, "required-agent:", "required-agent=", "required_agent:", "required_agent=", "required-agent-id:", "required-agent-id=", "owner-agent:", "owner-agent=", "owner_agent:", "owner_agent="),
			workspaceTaskPointerValue(task.ClaimAgentID),
		))
		if required == "" || required == agentID {
			return true
		}
	}
	return false
}

func agentWorkPatchQueueSubmitHandoffTaskID(projectID, branchID, headSHA, reviewDocKey string) string {
	seed := strings.Join([]string{projectID, branchID, headSHA, reviewDocKey}, "|")
	return agentWorkSanitizeRefSegment("task-patchq-submit-" + agentWorkCompactRefSegment("project", projectID) + "-" + agentWorkShortHash(seed))
}

func agentWorkOpenPatchQueueClaimStewardshipTaskExists(tasks []WorkspaceTaskRecord, dependencyBlocks map[string][]string, item ProjectPatchQueueItemRecord) bool {
	queueID := strings.ToLower(strings.TrimSpace(item.QueueID))
	itemID := strings.ToLower(strings.TrimSpace(item.ItemID))
	branchID := strings.ToLower(strings.TrimSpace(item.BranchID))
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if agentWorkPatchQueueReviewReceiptTask(task) {
			continue
		}
		if strings.TrimSpace(task.ProjectID) != strings.TrimSpace(item.ProjectID) {
			continue
		}
		text := strings.ToLower(strings.Join(append([]string{task.TaskID, task.Title, task.Description}, task.Tags...), "\n"))
		if !(strings.Contains(text, "patch_queue") || strings.Contains(text, "patch-queue") || strings.Contains(text, "patch queue")) {
			continue
		}
		if !(strings.Contains(text, "queue-stewardship") || strings.Contains(text, "claim-stewardship") || strings.Contains(text, "claimed-decision") || strings.Contains(text, "claim-expired")) {
			continue
		}
		if itemID != "" && !strings.Contains(text, itemID) {
			continue
		}
		if queueID != "" && !strings.Contains(text, queueID) {
			continue
		}
		if branchID != "" && !strings.Contains(text, branchID) {
			continue
		}
		return true
	}
	return false
}

func agentWorkPatchQueueClaimStewardshipPacket(candidate agentWorkPatchQueueClaimCandidate) *AgentWorkPacket {
	item := candidate.Item
	branch := candidate.Branch
	operationBound := candidate.OperationBindingPresent
	allowedActions := []string{"claim"}
	if operationBound {
		allowedActions = append(allowedActions, "accept", "reject", "block", "cancel")
	} else {
		allowedActions = append(allowedActions, "reviewer_advisory", "release", "accept", "reject", "block", "cancel")
	}
	claimState := "expired_or_reclaimable"
	if candidate.ClaimActive {
		claimState = "active_self_claim"
	}
	releaseGuidance := "If the claim is expired, first reclaim it; do not release a foreign claim."
	if operationBound {
		releaseGuidance = "Operation binding exists; decide or cancel instead of releasing the item."
	}
	summary := fmt.Sprintf("CLAIMED patch queue item %s/%s is %s for %s and still lacks a terminal decision; create explicit lifecycle stewardship.", strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), claimState, strings.TrimSpace(item.ClaimedBy))
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_claim_stewardship_available",
		ProjectID:           strings.TrimSpace(item.ProjectID),
		TaskKind:            string(model.TaskKindCoordination),
		ProjectLane:         "review",
		RequiresProjectGate: false,
		CoordinationState:   "claimed_patch_queue_item_needs_lifecycle",
		PreferredTransition: "project_patch_queue_lifecycle",
		WhyNow:              summary + " " + releaseGuidance,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: uniqueTrimmedAgentWork([]string{item.ReviewDocKey, item.EvidenceDocKey, item.DecisionDocKey}),
			AnchorBranchIDs:  uniqueTrimmedAgentWork([]string{item.BranchID}),
		},
		PatchQueueClaim: &AgentWorkPatchQueueClaim{
			ProjectID:               strings.TrimSpace(item.ProjectID),
			QueueID:                 strings.TrimSpace(item.QueueID),
			ItemID:                  strings.TrimSpace(item.ItemID),
			BranchID:                strings.TrimSpace(item.BranchID),
			BranchName:              strings.TrimSpace(firstNonEmpty(branch.BranchName, item.BranchID)),
			HeadSHA:                 strings.TrimSpace(item.HeadSHA),
			State:                   strings.TrimSpace(item.State),
			ClaimedBy:               strings.TrimSpace(item.ClaimedBy),
			ClaimExpiresAt:          strings.TrimSpace(item.ClaimExpiresAt),
			ClaimActive:             candidate.ClaimActive,
			OperationBindingPresent: operationBound,
			ReviewDocKey:            strings.TrimSpace(item.ReviewDocKey),
			EvidenceDocKey:          strings.TrimSpace(item.EvidenceDocKey),
			DecisionDocKey:          strings.TrimSpace(item.DecisionDocKey),
			AllowedActions:          allowedActions,
			Summary:                 summary,
		},
	}
}

func agentWorkShortHash(value string) string {
	hash := contentSHA256(strings.TrimSpace(value))
	if len(hash) > 16 {
		return hash[:16]
	}
	return hash
}

func agentWorkCompactRefSegment(prefix, value string) string {
	const maxRefSegmentLen = 32
	raw := firstNonEmpty(strings.TrimSpace(value), strings.TrimSpace(prefix), "x")
	segment := agentWorkSanitizeRefSegment(raw)
	if len(segment) <= maxRefSegmentLen {
		return segment
	}
	hash := contentSHA256(strings.TrimSpace(raw))
	if len(hash) > 10 {
		hash = hash[:10]
	}
	keep := maxRefSegmentLen - len(hash) - 1
	if keep < 1 {
		return hash[:maxRefSegmentLen]
	}
	head := strings.Trim(segment[:keep], "-.")
	if head == "" {
		head = agentWorkSanitizeRefSegment(firstNonEmpty(prefix, "x"))
	}
	if len(head) > keep {
		head = strings.Trim(head[:keep], "-.")
	}
	if head == "" {
		head = "x"
	}
	return head + "-" + hash
}

func agentWorkSanitizeRefSegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.'
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	segment := strings.Trim(b.String(), "-.")
	if segment == "" {
		segment = "x"
	}
	if len(segment) > 80 {
		segment = strings.Trim(segment[:80], "-.")
	}
	return segment
}

func (s *Store) agentWorkPatchQueueFreshSupersedeEvidenceDoc(ctx context.Context, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) (WorkspaceDocRecord, time.Time, bool, error) {
	referenceAt := firstNonEmpty(strings.TrimSpace(item.DecidedAt), strings.TrimSpace(item.UpdatedAt), strings.TrimSpace(item.CreatedAt))
	if referenceAt == "" {
		return WorkspaceDocRecord{}, time.Time{}, false, nil
	}
	referenceTime, err := time.Parse(time.RFC3339Nano, referenceAt)
	if err != nil {
		return WorkspaceDocRecord{}, time.Time{}, false, nil
	}
	summaries, err := s.ListWorkspaceDocs(ctx, workspaceID, false)
	if err != nil {
		return WorkspaceDocRecord{}, time.Time{}, false, err
	}
	var selected WorkspaceDocRecord
	var selectedTime time.Time
	found := false
	for _, summary := range summaries {
		if strings.TrimSpace(summary.DocKey) == strings.TrimSpace(item.DecisionDocKey) {
			continue
		}
		if strings.TrimSpace(summary.DocKey) == strings.TrimSpace(item.EvidenceDocKey) {
			continue
		}
		if agentWorkPatchQueueEvidenceKeyConsumedByTerminalSameHead(items, item, summary.DocKey) {
			continue
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(summary.UpdatedAt))
		if err != nil || !updatedAt.After(referenceTime) {
			continue
		}
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, summary.DocKey)
		if err != nil {
			return WorkspaceDocRecord{}, time.Time{}, false, err
		}
		if doc.ArchivedAt != nil {
			continue
		}
		consumedBasis, err := s.agentWorkPatchQueueEvidenceBasisConsumedByTerminalSameHead(ctx, workspaceID, doc, item, branch, items)
		if err != nil {
			return WorkspaceDocRecord{}, time.Time{}, false, err
		}
		if consumedBasis {
			continue
		}
		if !agentWorkPatchQueueEvidenceDocNamesSupersession(doc, item, branch) {
			continue
		}
		if !found || updatedAt.After(selectedTime) || (updatedAt.Equal(selectedTime) && strings.TrimSpace(doc.DocKey) < strings.TrimSpace(selected.DocKey)) {
			selected = doc
			selectedTime = updatedAt
			found = true
		}
	}
	return selected, referenceTime, found, nil
}

func agentWorkPatchQueueEvidenceDocNamesSupersession(doc WorkspaceDocRecord, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	if projectPatchQueueSupersessionEvidenceDocIsCoordinationResponse(doc) {
		return false
	}
	if projectPatchQueueSupersessionEvidenceDocIsAgentState(doc) {
		return false
	}
	if projectPatchQueueSupersessionEvidenceDocIsReflectiveSummary(doc) {
		return false
	}
	if projectPatchQueueSupersessionEvidenceDocIsTaskBrief(doc) {
		return false
	}
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	if projectPatchQueueSupersessionEvidenceMissingTargetRef(text, item, branch) != "" {
		return false
	}
	if projectPatchQueueSupersessionEvidenceHasExplicitNegativeVerdict(text) {
		return false
	}
	hasPositiveValidation := projectPatchQueueSupersessionEvidenceHasPositiveValidation(text)
	if projectPatchQueueSupersessionEvidenceRejectsProgress(text) && !projectPatchQueueSupersessionEvidenceClosesStaleBlocker(text, hasPositiveValidation) {
		return false
	}
	if len(projectPatchQueueSupersessionVisualAcceptanceMissingRequirements(text, item)) > 0 {
		return false
	}
	if !hasPositiveValidation {
		return false
	}
	return true
}

func agentWorkPatchQueueEvidenceKeyConsumedByTerminalSameHead(items []ProjectPatchQueueItemRecord, current ProjectPatchQueueItemRecord, evidenceDocKey string) bool {
	evidenceDocKey = strings.TrimSpace(evidenceDocKey)
	if evidenceDocKey == "" {
		return false
	}
	currentQueueID := strings.TrimSpace(current.QueueID)
	currentItemID := strings.TrimSpace(current.ItemID)
	currentBranchID := strings.TrimSpace(current.BranchID)
	currentHeadSHA := strings.TrimSpace(current.HeadSHA)
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) == currentQueueID && strings.TrimSpace(item.ItemID) == currentItemID {
			continue
		}
		if strings.TrimSpace(item.BranchID) != currentBranchID || strings.TrimSpace(item.HeadSHA) != currentHeadSHA {
			continue
		}
		if strings.TrimSpace(item.EvidenceDocKey) != evidenceDocKey {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
			return true
		}
	}
	return false
}

func (s *Store) agentWorkPatchQueueEvidenceBasisConsumedByTerminalSameHead(ctx context.Context, workspaceID string, candidateDoc WorkspaceDocRecord, current ProjectPatchQueueItemRecord, branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) (bool, error) {
	refs := []ProjectPatchQueueItemRecord{current}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) == strings.TrimSpace(current.BranchID) &&
			strings.TrimSpace(item.HeadSHA) == strings.TrimSpace(current.HeadSHA) {
			refs = append(refs, item)
		}
	}
	candidateDigest := projectPatchQueueSupersessionEvidenceBasisDigest(candidateDoc, refs, branch)
	if candidateDigest == "" {
		return false, nil
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != strings.TrimSpace(current.BranchID) || strings.TrimSpace(item.HeadSHA) != strings.TrimSpace(current.HeadSHA) {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
		default:
			continue
		}
		docKey := strings.TrimSpace(item.EvidenceDocKey)
		if docKey == "" || docKey == strings.TrimSpace(candidateDoc.DocKey) {
			continue
		}
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return false, err
		}
		if projectPatchQueueSupersessionEvidenceBasisDigest(doc, refs, branch) == candidateDigest {
			return true, nil
		}
	}
	return false, nil
}

func agentWorkPatchQueueSupersedeAvailablePacket(candidate agentWorkPatchQueueSupersedeCandidate) *AgentWorkPacket {
	item := candidate.Item
	branch := candidate.Branch
	projectID := strings.TrimSpace(item.ProjectID)
	evidenceDocKey := strings.TrimSpace(candidate.EvidenceDoc.DocKey)
	newItemID := agentWorkPatchQueueSupersedeItemID(item, evidenceDocKey)
	summary := fmt.Sprintf("BLOCKED patch queue item %s/%s has fresh exact evidence %s for branch %s at head %s; create a live supersession item before re-review/integration.", strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), firstNonEmpty(evidenceDocKey, "unknown"), strings.TrimSpace(item.BranchID), strings.TrimSpace(item.HeadSHA))
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_supersede_available",
		ProjectID:           projectID,
		TaskKind:            model.TaskKindExecution,
		ProjectLane:         "integration",
		RequiresProjectGate: false,
		CoordinationState:   "blocked_queue_has_fresh_evidence",
		PreferredTransition: "create_or_claim_patch_queue_supersede_stewardship",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: uniqueTrimmedAgentWork([]string{
				evidenceDocKey,
				strings.TrimSpace(item.DecisionDocKey),
				strings.TrimSpace(item.ReviewDocKey),
				"project." + projectID + ".reflection_board",
			}),
			AnchorBranchIDs: uniqueTrimmedAgentWork([]string{strings.TrimSpace(item.BranchID)}),
		},
		PatchQueueSupersede: &AgentWorkPatchQueueSupersede{
			ProjectID:      projectID,
			QueueID:        strings.TrimSpace(item.QueueID),
			ItemID:         strings.TrimSpace(item.ItemID),
			BranchID:       strings.TrimSpace(item.BranchID),
			BranchName:     strings.TrimSpace(branch.BranchName),
			HeadSHA:        strings.TrimSpace(item.HeadSHA),
			NewItemID:      newItemID,
			EvidenceDocKey: evidenceDocKey,
			DecisionDocKey: strings.TrimSpace(item.DecisionDocKey),
			ReviewDocKey:   strings.TrimSpace(item.ReviewDocKey),
			Summary:        summary,
		},
		Decision: &AgentWorkDecisionPacket{
			NeededFrom:   "reviewer_or_integrator",
			DecisionType: "patch_queue_supersession",
		},
	}
}

func agentWorkPatchQueueSubmitHandoffPacket(candidate agentWorkPatchQueueSubmitHandoffCandidate) *AgentWorkPacket {
	branch := candidate.Branch
	projectID := strings.TrimSpace(branch.ProjectID)
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	reviewDocKey := strings.TrimSpace(branch.ReviewDocKey)
	ownerID := strings.TrimSpace(branch.AgentID)
	summary := fmt.Sprintf("READY_FOR_REVIEW branch %s at head %s has review_doc_key %s but no patch queue handoff item; branch owner %s must call project_patch_queue_submit before validation/integration treats this candidate as queued.", branchID, firstNonEmpty(headSHA, "unknown"), firstNonEmpty(reviewDocKey, "unknown"), firstNonEmpty(ownerID, "unknown"))
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_submit_handoff_available",
		ProjectID:           projectID,
		TaskKind:            model.TaskKindExecution,
		ProjectLane:         "integration",
		RequiresProjectGate: false,
		CoordinationState:   "ready_branch_missing_patch_queue_handoff",
		PreferredTransition: "create_or_claim_owner_bound_patch_queue_submit",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: uniqueTrimmedAgentWork([]string{
				reviewDocKey,
				"project." + projectID + ".reflection_board",
			}),
			AnchorBranchIDs: uniqueTrimmedAgentWork([]string{branchID}),
		},
		OwnerBound: &AgentWorkOwnerBoundPacket{
			Kind:            "patch_queue_submit",
			RequiredAgentID: ownerID,
			BranchID:        branchID,
			BranchName:      strings.TrimSpace(branch.BranchName),
			HeadSHA:         headSHA,
			ReviewDocKey:    reviewDocKey,
		},
		Gate: &AgentWorkGatePacket{
			GateState:  "open",
			GateType:   "patch_queue_handoff_receipt",
			NeededFrom: firstNonEmpty(ownerID, "branch_owner"),
			Summary:    summary,
		},
	}
}

func agentWorkPatchQueueSupersedeItemID(item ProjectPatchQueueItemRecord, evidenceDocKey string) string {
	seed := strings.Join([]string{
		strings.TrimSpace(item.QueueID),
		strings.TrimSpace(item.ItemID),
		strings.TrimSpace(item.BranchID),
		strings.TrimSpace(item.HeadSHA),
		strings.TrimSpace(evidenceDocKey),
	}, "|")
	hash := contentSHA256(seed)
	if len(hash) > 12 {
		hash = hash[:12]
	}
	return "supersede-" + hash
}

func (s *Store) agentMaySelectProjectRoleLaneTask(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (bool, error) {
	requiredRoles := projectClaimRequiredRoleTypesForLane(task.ProjectLane)
	if len(requiredRoles) == 0 || strings.TrimSpace(task.ProjectID) == "" {
		return true, nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	agentID = strings.TrimSpace(agentID)
	lead, hasActiveLead, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		return false, err
	}
	if hasActiveLead && strings.TrimSpace(lead.AgentID) == agentID {
		return true, nil
	}
	roles, err := s.ListProjectRoles(ctx, workspaceID, projectID, false)
	if err != nil {
		return false, err
	}
	hasActiveExecutionRoles := false
	for _, role := range roles {
		roleType := strings.ToUpper(strings.TrimSpace(role.RoleType))
		if roleType == ProjectRoleStrategicLead {
			continue
		}
		hasActiveExecutionRoles = true
		if strings.TrimSpace(role.AgentID) != agentID || !projectClaimRoleTypeAllowed(roleType, requiredRoles) {
			continue
		}
		if roleType == ProjectRoleImplementer && len(writeScopePaths(role.WriteScopeJSON)) == 0 {
			continue
		}
		return true, nil
	}
	if !hasActiveExecutionRoles {
		agent, err := s.GetAgent(ctx, workspaceID, agentID)
		if err != nil {
			if errors.Is(err, ErrAgentNotFound) {
				return false, nil
			}
			return false, err
		}
		profile, err := s.GetAgentProfile(ctx, workspaceID, agentID)
		if err != nil {
			return false, err
		}
		if agentWorkProfileOrRegisteredRoleAllowsProjectLane(profile, agent, task.ProjectLane) {
			return true, nil
		}
	}
	return !hasActiveLead && !hasActiveExecutionRoles && projectClaimRequiredRolesAllowBootstrap(requiredRoles), nil
}

func agentProfileAllowsProjectRoleLane(profile AgentProfileRecord, requiredRoles []string) bool {
	mode := agentProfileFreshSelectionMode(profile)
	for _, role := range requiredRoles {
		switch strings.ToUpper(strings.TrimSpace(role)) {
		case ProjectRoleImplementer:
			if mode == "implementation" {
				return true
			}
		case ProjectRoleReviewer:
			if mode == "review" {
				return true
			}
		case ProjectRoleIntegrator:
			if mode == "synthesis" {
				return true
			}
		}
	}
	return false
}

func agentWorkPatchQueueRefsFromTask(task WorkspaceTaskRecord) (string, string, string) {
	texts := []string{task.Title, task.Description}
	queueID := firstNonEmpty(agentWorkTaskRequirementString(task, "queue_id"), agentWorkTaskRequirementString(task, "patch_queue_id"), agentWorkTaskTextFieldValue(texts, "queue_id"), agentWorkTaskTextFieldValue(texts, "patch_queue"))
	itemID := firstNonEmpty(agentWorkTaskRequirementString(task, "item_id"), agentWorkTaskRequirementString(task, "patch_item_id"), agentWorkTaskTextFieldValue(texts, "item_id"), agentWorkTaskTextFieldValue(texts, "patch_item"))
	branchID := firstNonEmpty(agentWorkTaskRequirementString(task, "branch_id"), agentWorkTaskRequirementString(task, "target_branch_id"), agentWorkTaskTextFieldValue(texts, "branch_id"))
	if branchID == "" {
		branchID = agentWorkTaskTextFieldValue(texts, "Branch ID")
	}
	if queueID == "" || itemID == "" {
		combined := agentWorkTaskTextFieldValue(texts, "Patch queue")
		if left, right, ok := strings.Cut(combined, "/"); ok {
			if queueID == "" {
				queueID = strings.TrimSpace(left)
			}
			if itemID == "" {
				itemID = strings.TrimSpace(right)
			}
		}
	}
	return strings.TrimSpace(queueID), strings.TrimSpace(itemID), strings.TrimSpace(branchID)
}

type agentWorkOwnerBoundRequirement struct {
	Kind            string
	ProjectID       string
	QueueID         string
	ItemID          string
	BranchID        string
	BranchName      string
	RequiredAgentID string
	RepairNeeded    bool
	Reason          string
}

func (s *Store) agentWorkOwnerBoundRequirement(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (agentWorkOwnerBoundRequirement, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" || !agentWorkTaskHasOwnerBoundSignal(task) {
		return agentWorkOwnerBoundRequirement{}, false, nil
	}
	queueID, itemID, branchID := agentWorkPatchQueueRefsFromTask(task)
	implicitKind := agentWorkImplicitOwnerBoundKind(task)
	req := agentWorkOwnerBoundRequirement{
		Kind:            firstNonEmpty(agentWorkTaskTagValue(task, "owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind="), implicitKind, "patch_queue_submit"),
		ProjectID:       projectID,
		QueueID:         firstNonEmpty(agentWorkTaskTagValue(task, "queue:", "queue=", "queue-id:", "queue-id=", "queue_id:", "queue_id="), queueID),
		ItemID:          firstNonEmpty(agentWorkTaskTagValue(task, "item:", "item=", "item-id:", "item-id=", "item_id:", "item_id="), itemID),
		BranchID:        firstNonEmpty(agentWorkTaskTagValue(task, "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=", "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id="), branchID, workspaceTaskPointerValue(task.ClaimBranchID)),
		BranchName:      firstNonEmpty(agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "branch_name"), agentWorkTaskTextFieldValue([]string{task.Title, task.Description}, "Branch name")),
		RequiredAgentID: agentWorkTaskTagValue(task, "required-agent:", "required-agent=", "required_agent:", "required_agent=", "required-agent-id:", "required-agent-id=", "owner-agent:", "owner-agent=", "owner_agent:", "owner_agent="),
	}
	if strings.TrimSpace(req.BranchID) == "" && (strings.TrimSpace(req.QueueID) != "" || strings.TrimSpace(req.ItemID) != "") {
		if strings.TrimSpace(req.QueueID) == "" || strings.TrimSpace(req.ItemID) == "" {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference must include both queue_id and item_id when branch_id is absent"
			return req, true, nil
		}
		if item, ok, err := s.agentWorkOwnerBoundPatchQueueItem(ctx, workspaceID, projectID, req.QueueID, req.ItemID); err != nil {
			return agentWorkOwnerBoundRequirement{}, false, err
		} else if ok {
			req.QueueID = firstNonEmpty(req.QueueID, strings.TrimSpace(item.QueueID))
			req.ItemID = firstNonEmpty(req.ItemID, strings.TrimSpace(item.ItemID))
			req.BranchID = strings.TrimSpace(item.BranchID)
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound patch queue reference did not resolve to a branch"
			return req, true, nil
		}
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       projectID,
		IncludeInactive: true,
	})
	if err != nil {
		return agentWorkOwnerBoundRequirement{}, false, err
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "active_lane_publication") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) == "" {
		if branch, ok, ambiguous := agentWorkOwnerBoundUniqueOpenBranchForProject(branches); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task matches multiple open project branches"
			return req, true, nil
		} else {
			req.RepairNeeded = true
			req.Reason = "active-lane publication task has no open project branch owner"
			return req, true, nil
		}
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") &&
		strings.TrimSpace(req.BranchID) == "" &&
		strings.TrimSpace(req.BranchName) == "" &&
		strings.TrimSpace(req.RequiredAgentID) != "" {
		if branch, ok, ambiguous := agentWorkOwnerBoundUniqueOpenBranchForOwner(branches, req.RequiredAgentID); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = fmt.Sprintf("owner-bound patch queue submit task matches multiple open branches for required agent %s", strings.TrimSpace(req.RequiredAgentID))
			return req, true, nil
		}
	}
	if strings.TrimSpace(req.BranchID) == "" && strings.TrimSpace(req.BranchName) == "" {
		if branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task); ok {
			req.BranchID = strings.TrimSpace(branch.BranchID)
			req.BranchName = strings.TrimSpace(branch.BranchName)
		} else if ambiguous {
			req.RepairNeeded = true
			req.Reason = "owner-bound task mentions multiple registered branches"
			return req, true, nil
		}
	}
	if branch, ok := agentWorkOwnerBoundResolveBranch(branches, req.BranchID, req.BranchName); ok {
		req.BranchID = strings.TrimSpace(branch.BranchID)
		req.BranchName = strings.TrimSpace(branch.BranchName)
		if owner := strings.TrimSpace(branch.AgentID); owner != "" {
			if taggedOwner := strings.TrimSpace(req.RequiredAgentID); taggedOwner != "" && taggedOwner != owner {
				req.RequiredAgentID = owner
				req.RepairNeeded = true
				req.Reason = fmt.Sprintf("owner-bound required agent %s conflicts with branch owner %s", taggedOwner, owner)
			} else {
				req.RequiredAgentID = owner
			}
		} else {
			req.RepairNeeded = true
			req.Reason = "owner-bound branch has no recorded owner"
		}
	} else if strings.TrimSpace(req.BranchID) != "" {
		req.RepairNeeded = true
		req.Reason = "owner-bound branch is not registered in project coordination"
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") && strings.TrimSpace(req.BranchID) == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound patch queue submit task does not identify a concrete branch"
	}
	if strings.TrimSpace(req.RequiredAgentID) == "" && !req.RepairNeeded {
		req.RepairNeeded = true
		req.Reason = "owner-bound task does not identify a required agent"
	}
	return req, true, nil
}

func (s *Store) agentWorkOwnerBoundPatchQueueItem(ctx context.Context, workspaceID, projectID, queueID, itemID string) (ProjectPatchQueueItemRecord, bool, error) {
	items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
		WorkspaceID: strings.TrimSpace(workspaceID),
		ProjectID:   strings.TrimSpace(projectID),
	})
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		return item, true, nil
	}
	return ProjectPatchQueueItemRecord{}, false, nil
}

func agentWorkOwnerBoundResolveBranch(branches []ProjectBranchRecord, branchID, branchName string) (ProjectBranchRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	branchName = strings.TrimSpace(branchName)
	for _, branch := range branches {
		if branchID != "" && strings.TrimSpace(branch.BranchID) == branchID {
			return branch, true
		}
	}
	if branchName != "" {
		var live []ProjectBranchRecord
		for _, branch := range branches {
			if strings.TrimSpace(branch.BranchName) != branchName {
				continue
			}
			if taskClaimRevisionSourceBranchStatusLive(branch.Status) {
				live = append(live, branch)
			}
		}
		if len(live) == 1 {
			return live[0], true
		}
	}
	return ProjectBranchRecord{}, false
}

func agentWorkOwnerBoundUniqueOpenBranchForOwner(branches []ProjectBranchRecord, ownerAgentID string) (ProjectBranchRecord, bool, bool) {
	ownerAgentID = strings.TrimSpace(ownerAgentID)
	if ownerAgentID == "" {
		return ProjectBranchRecord{}, false, false
	}
	var preferred []ProjectBranchRecord
	var fallback []ProjectBranchRecord
	for _, branch := range branches {
		if strings.TrimSpace(branch.AgentID) != ownerAgentID {
			continue
		}
		if projectBranchStatusIsTerminal(branch.Status) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(branch.Status), ProjectBranchStatusReadyForReview) {
			preferred = append(preferred, branch)
			continue
		}
		fallback = append(fallback, branch)
	}
	switch {
	case len(preferred) == 1:
		return preferred[0], true, false
	case len(preferred) > 1:
		return ProjectBranchRecord{}, false, true
	case len(fallback) == 1:
		return fallback[0], true, false
	case len(fallback) > 1:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, false
	}
}

func agentWorkOwnerBoundUniqueOpenBranchForProject(branches []ProjectBranchRecord) (ProjectBranchRecord, bool, bool) {
	var preferred []ProjectBranchRecord
	var fallback []ProjectBranchRecord
	for _, branch := range branches {
		if projectBranchStatusIsTerminal(branch.Status) {
			continue
		}
		if strings.TrimSpace(branch.AgentID) == "" {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.EqualFold(strings.TrimSpace(branch.Status), ProjectBranchStatusActive) {
			preferred = append(preferred, branch)
			continue
		}
		fallback = append(fallback, branch)
	}
	switch {
	case len(preferred) == 1:
		return preferred[0], true, false
	case len(preferred) > 1:
		return ProjectBranchRecord{}, false, true
	case len(fallback) == 1:
		return fallback[0], true, false
	case len(fallback) > 1:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, false
	}
}

func agentWorkOwnerBoundBranchMentionedInTask(branches []ProjectBranchRecord, task WorkspaceTaskRecord) (ProjectBranchRecord, bool, bool) {
	identityText := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.Join(task.Tags, " "),
	}, "\n"))
	if branch, ok, ambiguous := agentWorkOwnerBoundBranchIDMentionedInText(branches, identityText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	if branch, ok, ambiguous := agentWorkOwnerBoundBranchNameMentionedInText(branches, identityText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	fullText := strings.ToLower(strings.Join([]string{
		identityText,
		strings.TrimSpace(task.Description),
	}, "\n"))
	if branch, ok, ambiguous := agentWorkOwnerBoundBranchIDMentionedInText(branches, fullText); ok || ambiguous {
		return branch, ok, ambiguous
	}
	return agentWorkOwnerBoundBranchNameMentionedInText(branches, fullText)
}

func agentWorkOwnerBoundBranchIDMentionedInText(branches []ProjectBranchRecord, text string) (ProjectBranchRecord, bool, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return ProjectBranchRecord{}, false, false
	}
	var matches []ProjectBranchRecord
	seen := map[string]struct{}{}
	for _, branch := range branches {
		branchID := strings.TrimSpace(branch.BranchID)
		if branchID == "" {
			continue
		}
		if !agentWorkOwnerBoundTextContainsIdentifier(text, branchID) {
			continue
		}
		if _, ok := seen[branchID]; ok {
			continue
		}
		seen[branchID] = struct{}{}
		matches = append(matches, branch)
	}
	switch len(matches) {
	case 0:
		return ProjectBranchRecord{}, false, false
	case 1:
		return matches[0], true, false
	default:
		return ProjectBranchRecord{}, false, true
	}
}

func agentWorkOwnerBoundBranchNameMentionedInText(branches []ProjectBranchRecord, text string) (ProjectBranchRecord, bool, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return ProjectBranchRecord{}, false, false
	}
	var matches []ProjectBranchRecord
	seen := map[string]struct{}{}
	for _, branch := range branches {
		branchID := strings.TrimSpace(branch.BranchID)
		branchName := strings.TrimSpace(branch.BranchName)
		if branchName == "" {
			continue
		}
		if !agentWorkOwnerBoundTextContainsIdentifier(text, branchName) {
			continue
		}
		key := firstNonEmpty(branchID, branchName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		matches = append(matches, branch)
	}
	return agentWorkOwnerBoundSelectMentionedBranch(matches)
}

func agentWorkOwnerBoundSelectMentionedBranch(matches []ProjectBranchRecord) (ProjectBranchRecord, bool, bool) {
	switch len(matches) {
	case 0:
		return ProjectBranchRecord{}, false, false
	case 1:
		return matches[0], true, false
	}
	var live []ProjectBranchRecord
	for _, branch := range matches {
		if !projectBranchStatusIsTerminal(branch.Status) {
			live = append(live, branch)
		}
	}
	switch len(live) {
	case 1:
		return live[0], true, false
	case 0:
		return ProjectBranchRecord{}, false, true
	default:
		return ProjectBranchRecord{}, false, true
	}
}

func agentWorkOwnerBoundTextContainsIdentifier(text, identifier string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	identifier = strings.ToLower(strings.TrimSpace(identifier))
	if text == "" || identifier == "" {
		return false
	}
	offset := 0
	for {
		idx := strings.Index(text[offset:], identifier)
		if idx < 0 {
			return false
		}
		start := offset + idx
		end := start + len(identifier)
		if agentWorkOwnerBoundIdentifierBoundary(text, start-1) && agentWorkOwnerBoundIdentifierBoundary(text, end) {
			return true
		}
		offset = start + 1
	}
}

func agentWorkOwnerBoundIdentifierBoundary(text string, idx int) bool {
	if idx < 0 || idx >= len(text) {
		return true
	}
	ch := text[idx]
	return !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '/' || ch == '.')
}

func agentWorkTaskHasOwnerBoundSignal(task WorkspaceTaskRecord) bool {
	if agentWorkTaskHasTag(task, "owner-bound") ||
		agentWorkTaskHasTag(task, "owner-submit", "owner_submit", "branch-owner-submit", "branch_owner_submit") ||
		agentWorkTaskHasTagPrefix(task,
			"owner-bound-kind:", "owner-bound-kind=", "owner_bound_kind:", "owner_bound_kind=",
			"required-agent:", "required-agent=", "required_agent:", "required_agent=",
			"required-agent-id:", "required-agent-id=",
			"owner-agent:", "owner-agent=", "owner_agent:", "owner_agent=",
			"owner-branch:", "owner-branch=", "owner_branch:", "owner_branch=",
		) {
		return true
	}
	// Free-text owner-bound detection intentionally ignores Description: ambient
	// reflection tasks embed historical open-task hints there, and those quotes
	// must not turn the reflection task itself into a branch-owner-only submit.
	fullText := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.TaskTemplate),
		strings.TrimSpace(task.ProjectLane),
		strings.Join(task.Tags, " "),
	}, "\n"))
	return agentWorkTextHasPositiveOwnerBoundSignal(fullText) || agentWorkTaskLooksActiveLanePublication(task)
}

func agentWorkImplicitOwnerBoundKind(task WorkspaceTaskRecord) string {
	if agentWorkTaskLooksActiveLanePublication(task) {
		return "active_lane_publication"
	}
	return ""
}

func agentWorkTaskLooksActiveLanePublication(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if agentWorkTaskHasTag(task,
		"publication-repair",
		"publication_repair",
		"publication:repair",
		"publication=repair",
	) || agentWorkTaskHasTagPrefix(task,
		"publication-repair:",
		"publication-repair=",
		"publication_repair:",
		"publication_repair=",
	) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.TaskTemplate),
		strings.TrimSpace(task.ProjectLane),
		strings.Join(task.Tags, " "),
	}, "\n"))
	if strings.TrimSpace(text) == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.ProjectLane), "integration") && agentWorkTextContainsAny(text, []string{
		"integrate",
		"integration convergence",
		"implementation lanes",
		"merge lanes",
		"merge lane",
		"cross-lane",
		"cross lane",
	}) {
		return false
	}
	return agentWorkTextContainsAny(text, []string{
		"candidate provenance",
		"publish provenance",
		"provenance status",
		"provenance review",
		"candidate publication",
		"candidate-publication",
		"runnable candidate publication",
		"runnable-candidate-publication",
		"publish candidate",
		"publish runnable candidate",
		"publish exact runnable",
		"publish exact runnable candidate",
		"publish review-ready",
		"publish review ready",
		"review-ready publication",
		"review ready publication",
		"publish candidate evidence",
		"publication-gap",
		"publication gap",
		"lane provenance",
		"implementation lane provenance",
		"publish durable provenance",
		"publish current status",
		"publish current evidence",
		"publication repair follow-up",
		"publication-repair follow-up",
	})
}

func agentWorkTextHasPositiveOwnerBoundSignal(fullText string) bool {
	fullText = strings.ToLower(strings.TrimSpace(fullText))
	if fullText == "" {
		return false
	}
	for _, negated := range []string{
		"not owner-only",
		"not owner only",
		"not owner-bound",
		"not owner bound",
		"not branch-owner-only",
		"not branch owner only",
		"is not owner-only",
		"is not owner only",
		"is not owner-bound",
		"is not owner bound",
		"is not branch-owner-only",
		"is not branch owner only",
		"without claiming owner-only",
		"without claiming owner only",
		"without claiming owner-submit",
		"without claiming owner submit",
		"without claiming branch-owner-only",
		"without claiming branch owner only",
		"without claiming branch-owner-submit",
		"without claiming branch owner submit",
		"not owner-submit",
		"not owner submit",
		"not branch-owner-submit",
		"not branch owner submit",
	} {
		if strings.Contains(fullText, negated) {
			return false
		}
	}
	return strings.Contains(fullText, "owner-only") ||
		strings.Contains(fullText, "owner only") ||
		strings.Contains(fullText, "branch-owner-only") ||
		strings.Contains(fullText, "branch owner only") ||
		strings.Contains(fullText, "owner requeue submit") ||
		strings.Contains(fullText, "branch owner submit")
}

func agentWorkTaskTagValue(task WorkspaceTaskRecord, prefixes ...string) string {
	for _, existing := range task.Tags {
		trimmed := strings.TrimSpace(existing)
		lower := strings.ToLower(trimmed)
		for _, prefix := range prefixes {
			prefix = strings.ToLower(strings.TrimSpace(prefix))
			if prefix == "" || !strings.HasPrefix(lower, prefix) {
				continue
			}
			if value := strings.TrimSpace(trimmed[len(prefix):]); value != "" {
				return strings.Trim(value, "`'\"")
			}
		}
	}
	return ""
}

func (s *Store) projectOwnerBoundSelectionBlock(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task)
	if err != nil || !ok {
		return nil, false, err
	}
	claimAgentID := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimAgentID))
	claimStatus := strings.ToUpper(strings.TrimSpace(workspaceTaskPointerValue(task.ClaimStatus)))
	if claimAgentID != "" && claimAgentID != strings.TrimSpace(req.RequiredAgentID) && claimStatus == model.TaskClaimStatusClaimed {
		if allowed, err := s.agentMaySeeOwnerBoundWrongClaimRepair(ctx, workspaceID, agentID, req); err != nil {
			return nil, false, err
		} else if allowed {
			return projectOwnerBoundWrongClaimPacket(task, req, claimAgentID), true, nil
		}
	}
	if req.RepairNeeded || strings.TrimSpace(req.RequiredAgentID) == "" {
		return projectOwnerBoundRepairRequiredPacket(task, req), true, nil
	}
	if strings.TrimSpace(agentID) != strings.TrimSpace(req.RequiredAgentID) {
		return projectOwnerBoundAgentRequiredPacket(task, req), true, nil
	}
	if packet, blocked, err := s.projectOwnerBoundActiveBranchSelectionBlock(ctx, workspaceID, agentID, task, req); err != nil || blocked {
		return packet, blocked, err
	}
	return nil, false, nil
}

func (s *Store) agentWorkActiveBranchSelectionBlock(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		return nil, false, nil
	}
	if req, ok, err := s.agentWorkOwnerBoundRequirement(ctx, workspaceID, task); err != nil {
		return nil, false, err
	} else if ok && strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") {
		return nil, false, nil
	}
	if agentWorkPatchQueueDecisionContinuationTask(task) &&
		!strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision") {
		return nil, false, nil
	}
	if agentWorkPatchQueueReviewTask(task) {
		return nil, false, nil
	}
	if agentWorkProjectLaneIsValidation(task.ProjectLane) &&
		strings.TrimSpace(workspaceTaskPointerValue(task.ClaimBranchID)) == "" &&
		!agentWorkPatchQueueRevisionFollowupTask(task) {
		return nil, false, nil
	}
	_, _, branchID := agentWorkPatchQueueRefsFromTask(task)
	branchID = firstNonEmpty(branchID, workspaceTaskPointerValue(task.ClaimBranchID))
	if strings.TrimSpace(branchID) == "" {
		return nil, false, nil
	}
	return s.projectOwnerBoundActiveBranchSelectionBlock(ctx, workspaceID, agentID, task, agentWorkOwnerBoundRequirement{
		ProjectID: projectID,
		BranchID:  branchID,
	})
}

func (s *Store) projectOwnerBoundActiveBranchSelectionBlock(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement) (*AgentWorkPacket, bool, error) {
	branchID := strings.TrimSpace(req.BranchID)
	projectID := strings.TrimSpace(req.ProjectID)
	if branchID == "" || projectID == "" {
		return nil, false, nil
	}
	if strings.EqualFold(strings.TrimSpace(req.Kind), "patch_queue_submit") {
		return nil, false, nil
	}
	branch, ok, err := s.agentWorkProjectBranchByID(ctx, workspaceID, projectID, branchID)
	if err != nil || !ok {
		return nil, false, err
	}
	if strings.TrimSpace(branch.AgentID) != strings.TrimSpace(agentID) || !projectBranchOwnsWriteScope(branch) {
		return nil, false, nil
	}
	taskID := strings.TrimSpace(task.TaskID)
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	if activeTaskID == "" && activeClaimID == "" {
		return nil, false, nil
	}
	if activeTaskID == taskID || activeClaimID == taskID {
		return nil, false, nil
	}
	var branchItems []ProjectPatchQueueItemRecord
	branchItemsLoaded := false
	loadBranchItems := func() error {
		if branchItemsLoaded {
			return nil
		}
		items, err := s.ListProjectPatchQueueItems(ctx, ProjectPatchQueueListFilter{
			WorkspaceID: strings.TrimSpace(workspaceID),
			ProjectID:   projectID,
			BranchID:    branchID,
		})
		if err != nil {
			return err
		}
		branchItems = items
		branchItemsLoaded = true
		return nil
	}
	if agentWorkPatchQueueRevisionFollowupTask(task) {
		if err := loadBranchItems(); err != nil {
			return nil, false, err
		}
		if agentWorkPatchQueueRevisionFollowupRebindsActiveBranch(task, branch, branchItems) {
			if strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
				return nil, false, nil
			}
			if agentWorkPatchQueueDecisionContinuationTask(task) &&
				strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision") {
				terminal, err := s.projectBranchActiveTaskTerminalForReadModel(ctx, branch)
				if err != nil {
					return nil, false, err
				}
				if terminal {
					return nil, false, nil
				}
			}
		}
	}
	inactive, err := s.projectBranchActiveRefsInactiveForReadModel(ctx, workspaceID, branch)
	if err != nil {
		return nil, false, err
	}
	if inactive {
		if err := loadBranchItems(); err != nil {
			return nil, false, err
		}
		if projectPatchQueueItemsReleaseBranchWriteScope(branchID, branch.HeadSHA, branchItems) {
			return nil, false, nil
		}
	}
	conflictTaskID := firstNonEmpty(activeTaskID, activeClaimID)
	summary := fmt.Sprintf("owner-bound branch %s is still active on task/claim %s; finish or terminalize that branch lane before selecting follow-up task %s", branchID, conflictTaskID, taskID)
	return projectImplementationClaimScopeBusyPacket(task, projectID, summary, conflictTaskID, branchID, strings.TrimSpace(branch.AgentID)), true, nil
}

func (s *Store) agentWorkProjectBranchByID(ctx context.Context, workspaceID, projectID, branchID string) (ProjectBranchRecord, bool, error) {
	branchID = strings.TrimSpace(branchID)
	if branchID == "" {
		return ProjectBranchRecord{}, false, nil
	}
	branches, err := s.ListProjectBranches(ctx, ProjectBranchListFilter{
		WorkspaceID:     strings.TrimSpace(workspaceID),
		ProjectID:       strings.TrimSpace(projectID),
		IncludeInactive: true,
	})
	if err != nil {
		return ProjectBranchRecord{}, false, err
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) == branchID {
			return branch, true, nil
		}
	}
	return ProjectBranchRecord{}, false, nil
}

func agentWorkPatchQueueRevisionFollowupRebindsActiveBranch(task WorkspaceTaskRecord, branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) bool {
	typedRevisionContinuation := agentWorkPatchQueueDecisionContinuationTask(task) &&
		strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision")
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) && !typedRevisionContinuation {
		return false
	}
	source, ok := agentWorkPatchQueueRevisionSourceItem(task, items)
	if !ok {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(source.State)) {
	case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected:
	default:
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(source.BranchID), strings.TrimSpace(branch.BranchID)) {
		return false
	}
	sourceHead := strings.TrimSpace(source.HeadSHA)
	branchHead := strings.TrimSpace(branch.HeadSHA)
	if sourceHead != "" && branchHead != "" && !strings.EqualFold(sourceHead, branchHead) {
		return false
	}
	return true
}

func (s *Store) projectBranchActiveTaskTerminalForReadModel(ctx context.Context, branch ProjectBranchRecord) (bool, error) {
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	if activeTaskID == "" {
		return false, nil
	}
	var status string
	if err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(status, '')
  FROM tasks
 WHERE task_id = ?`,
		activeTaskID,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load owner-bound branch active task terminal status: %w", err)
	}
	return isTerminalTaskStatus(status), nil
}

func (s *Store) projectBranchActiveRefsInactiveForReadModel(ctx context.Context, workspaceID string, branch ProjectBranchRecord) (bool, error) {
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	if activeTaskID == "" && activeClaimID == "" {
		return true, nil
	}
	activeTaskStatus := ""
	if activeTaskID != "" {
		if err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(status, '')
  FROM tasks
 WHERE task_id = ?`,
			activeTaskID,
		).Scan(&activeTaskStatus); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("load owner-bound branch active task status: %w", err)
		}
	}
	activeClaimStatus := ""
	if activeClaimID != "" {
		if err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(claim_status, '')
  FROM task_claims
 WHERE workspace_id = ?
   AND task_id = ?
   AND branch_id = ?
 ORDER BY updated_at DESC
 LIMIT 1`,
			strings.TrimSpace(workspaceID), activeClaimID, strings.TrimSpace(branch.BranchID),
		).Scan(&activeClaimStatus); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("load owner-bound branch active claim status: %w", err)
		}
	}
	return taskClaimBranchActiveRefsTerminalOrEmpty(activeTaskID, activeTaskStatus, activeClaimID, activeClaimStatus), nil
}

func (s *Store) agentMaySeeOwnerBoundWrongClaimRepair(ctx context.Context, workspaceID, agentID string, req agentWorkOwnerBoundRequirement) (bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false, nil
	}
	if agentID == strings.TrimSpace(req.RequiredAgentID) {
		return true, nil
	}
	lead, ok, err := s.GetActiveProjectStrategicLead(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(req.ProjectID))
	if err != nil {
		return false, err
	}
	return ok && strings.TrimSpace(lead.AgentID) == agentID, nil
}

func agentWorkTaskTextFieldValue(texts []string, key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return ""
	}
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			fieldKey, value, ok := strings.Cut(line, ":")
			if !ok {
				fieldKey, value, ok = strings.Cut(line, "=")
			}
			if ok && strings.ToLower(strings.TrimSpace(fieldKey)) == key {
				return strings.Trim(strings.TrimSpace(value), "`'\"")
			}
			if value := agentWorkInlineTaskTextFieldValue(line, key); value != "" {
				return value
			}
		}
	}
	return ""
}

func agentWorkInlineTaskTextFieldValue(text, key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return ""
	}
	lower := strings.ToLower(text)
	for _, sep := range []string{"=", ":"} {
		marker := key + sep
		idx := strings.Index(lower, marker)
		if idx < 0 {
			continue
		}
		value := strings.TrimLeft(text[idx+len(marker):], " \t`'\"")
		if value == "" {
			continue
		}
		end := len(value)
		for i, r := range value {
			if strings.ContainsRune(" \t\r\n,);]}`'\"", r) {
				end = i
				break
			}
		}
		if trimmed := strings.TrimSpace(value[:end]); trimmed != "" {
			return strings.Trim(trimmed, "`'\"")
		}
	}
	return ""
}

func agentWorkProfileWithAgentFallback(profile AgentProfileRecord, agent AgentRecord) AgentProfileRecord {
	role := strings.TrimSpace(agent.Role)
	if role == "" {
		return profile
	}
	if agentWorkProfileHasExplicitRoutingEvidence(profile) {
		return profile
	}
	if strings.TrimSpace(profile.Specialization) == "" {
		profile.Specialization = role
	}
	profile.Tags = uniqueSortedStrings(append(profile.Tags, role))
	return profile
}

func agentWorkProfileHasExplicitRoutingEvidence(profile AgentProfileRecord) bool {
	if strings.TrimSpace(profile.Specialization) != "" {
		return true
	}
	if strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "default_work_mode")) != "" ||
		strings.TrimSpace(agentProfileMetadataString(profile.Metadata, "primary_specialization")) != "" {
		return true
	}
	if agentProfileHasExactSelectionSignal(profile, []string{
		"synthesis",
		"synthesizer",
		"integration",
		"integrator",
		"review",
		"reviewer",
		"qa",
		"tester",
		"verifier",
		"strategy",
		"strategist",
		"project strategy",
		"planner",
		"coordination",
		"coordinator",
		"implementation",
		"implementer",
		"builder",
	}) {
		return true
	}
	return agentProfileHasImplementationMandate(profile)
}

func agentWorkProfileOrRegisteredRoleAllowsProjectLane(profile AgentProfileRecord, agent AgentRecord, projectLane string) bool {
	requiredRoles := projectClaimRequiredRoleTypesForLane(projectLane)
	if len(requiredRoles) == 0 {
		return false
	}
	if agentWorkProfileHasExplicitRoutingEvidence(profile) {
		return agentProfileAllowsProjectRoleLane(profile, requiredRoles)
	}
	if projectClaimRegisteredRoleAllowsLane(projectLane, agent.Role) {
		return true
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	return agentProfileAllowsProjectRoleLane(profile, requiredRoles)
}

func agentWorkProfileOrRegisteredRoleAllowsPatchQueueReview(profile AgentProfileRecord, agent AgentRecord) bool {
	requiredRoles := []string{ProjectRoleReviewer, ProjectRoleIntegrator}
	if agentWorkProfileHasExplicitRoutingEvidence(profile) {
		return agentProfileAllowsProjectRoleLane(profile, requiredRoles)
	}
	if projectPatchQueueRegisteredAgentRoleAllowsIntegration(agent.Role) {
		return true
	}
	profile = agentWorkProfileWithAgentFallback(profile, agent)
	return agentProfileAllowsProjectRoleLane(profile, requiredRoles)
}

func agentProfileFreshSelectionMode(profile AgentProfileRecord) string {
	if agentProfileHasExactSelectionSignal(profile, []string{
		"synthesis",
		"synthesizer",
		"integration",
		"integrator",
	}) {
		return "synthesis"
	}
	if agentProfileHasExactSelectionSignal(profile, []string{
		"review",
		"reviewer",
		"qa",
		"tester",
		"verifier",
	}) {
		return "review"
	}
	if agentProfileHasExactSelectionSignal(profile, []string{
		"strategy",
		"strategist",
		"project strategy",
		"planner",
		"coordination",
		"coordinator",
	}) {
		return "strategy"
	}
	if agentProfileHasExactSelectionSignal(profile, []string{
		"implementation",
		"implementer",
		"builder",
	}) {
		return "implementation"
	}
	if agentProfileHasImplementationMandate(profile) {
		return "implementation"
	}
	if agentProfileHasHighConfidenceReviewSignal(profile) {
		return "review"
	}
	if agentProfileHasSelectionSignal(profile, []string{
		"synthesis",
		"synthesizer",
		"documentation",
		"final report",
		"operator-facing summaries",
		"qa summaries",
	}) {
		return "synthesis"
	}
	if agentProfileHasSelectionSignal(profile, []string{
		"review",
		"reviewer",
		"qa",
		"quality assurance",
		"test design",
		"tester",
		"verification",
		"verifier",
		"critic",
		"critique",
		"visual qa",
		"visual acceptance",
		"usability defects",
		"accessibility verifier",
		"browser smoke",
		"bug finding",
		"acceptance evidence",
	}) {
		return "review"
	}
	if agentProfileHasSelectionSignal(profile, []string{
		"implementation",
		"implementer",
		"builder",
		"build",
		"developer",
		"software engineer",
		"frontend",
		"front-end",
		"backend",
		"fullstack",
		"full-stack",
		"state implementer",
		"data/model",
		"data model",
		"product slice",
		"local services",
		"integration glue",
		"small runnable artifacts",
		"coding",
		"code",
		"repair",
	}) {
		return "implementation"
	}
	if agentProfileHasSelectionSignal(profile, []string{
		"strategy",
		"strategist",
		"project strategy",
		"autonomous project strategy",
		"planning",
		"planner",
		"project framing",
		"task decomposition",
		"shared design docs",
		"coordination plan",
		"blocker surfacing",
	}) {
		return "strategy"
	}
	return "generalist"
}

func agentProfileHasHighConfidenceReviewSignal(profile AgentProfileRecord) bool {
	return agentProfileHasSelectionSignal(profile, []string{
		"review",
		"reviewer",
		"qa",
		"quality assurance",
		"test design",
		"tester",
		"verification",
		"verifier",
		"critic",
		"visual qa",
		"visual acceptance",
		"usability defects",
		"accessibility verifier",
		"browser smoke",
		"bug finding",
		"acceptance evidence",
	})
}

func agentProfileHasImplementationMandate(profile AgentProfileRecord) bool {
	return agentProfileHasStrictSelectionSignal(profile, []string{
		"implementer",
		"builder",
		"state implementer",
		"data/model implementer",
		"data model implementer",
	})
}

func agentProfileHasStrictSelectionSignal(profile AgentProfileRecord, signals []string) bool {
	for _, value := range agentProfileSelectionSignalValues(profile) {
		normalized := agentWorkNormalizeSemanticSignal(value)
		if normalized == "" {
			continue
		}
		for _, signal := range signals {
			signal = agentWorkNormalizeSemanticSignal(signal)
			if signal == "" {
				continue
			}
			if normalized == signal || agentWorkTextHasStrictSignal(normalized, signal) {
				return true
			}
		}
	}
	return false
}

func agentProfileHasExactSelectionSignal(profile AgentProfileRecord, signals []string) bool {
	for _, value := range agentProfileSelectionSignalValues(profile) {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		for _, signal := range signals {
			if normalized == strings.ToLower(strings.TrimSpace(signal)) {
				return true
			}
		}
	}
	return false
}

func agentProfileHasSelectionSignal(profile AgentProfileRecord, signals []string) bool {
	for _, value := range agentProfileSelectionSignalValues(profile) {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		for _, signal := range signals {
			signal = strings.ToLower(strings.TrimSpace(signal))
			if normalized == signal || agentWorkTextHasSignal(normalized, signal) {
				return true
			}
		}
	}
	return false
}

func agentWorkTextHasStrictSignal(text, signal string) bool {
	textTokens := agentWorkSemanticSignalTokens(text)
	signalTokens := agentWorkSemanticSignalTokens(signal)
	if len(textTokens) == 0 || len(signalTokens) == 0 {
		return false
	}
	for _, signalToken := range signalTokens {
		matched := false
		for _, textToken := range textTokens {
			if strings.EqualFold(strings.TrimSpace(textToken), strings.TrimSpace(signalToken)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func agentProfileSelectionSignalValues(profile AgentProfileRecord) []string {
	values := []string{
		profile.Specialization,
		agentProfileMetadataString(profile.Metadata, "default_work_mode"),
		agentProfileMetadataString(profile.Metadata, "primary_specialization"),
	}
	values = append(values, profile.Tags...)
	return values
}

func agentWorkTaskLooksReviewScoped(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if agentWorkTaskStructuredProjectEvidenceWork(task) && agentWorkTaskRequirementsContainWorkMode(task, "review") {
		return true
	}
	if agentWorkTaskLooksActionRouteValidationScoped(task) {
		return true
	}
	if agentWorkProjectLaneIsReview(lane) {
		return true
	}
	if lane != "" && (agentWorkProjectLaneBlocksRoleSignalRouting(lane) || agentWorkProjectLaneIsSynthesis(lane) || projectLaneRequiresImplementationGate(lane)) {
		return false
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return false
	}
	if agentWorkTextContainsAny(agentWorkTaskSearchText(task), []string{
		"review",
		"qa",
		"test",
		"testing",
		"bug",
		"verify",
		"verification",
		"acceptance",
		"evidence",
		"proof",
		"smoke",
		"audit",
		"critique",
	}) {
		return true
	}
	if agentWorkTextContainsAny(agentWorkTaskTagText(task), []string{
		"review",
		"reviewer",
		"qa",
		"quality",
		"test",
		"testing",
		"validation",
		"audit",
		"critique",
		"smoke",
	}) {
		return true
	}
	return agentWorkTextContainsAny(agentWorkTaskDescriptionText(task), []string{
		"audit",
		"browser smoke",
		"smoke test",
		"acceptance evidence",
		"ux regression",
		"quality pass",
		"spec-fidelity",
		"accessibility pass",
		"verification pass",
		"verify output",
	})
}

func agentWorkTaskLooksSynthesisScoped(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if agentWorkTaskStructuredProjectEvidenceWork(task) && agentWorkTaskRequirementsContainWorkMode(task, "synthesis") {
		return true
	}
	if agentWorkProjectLaneIsSynthesis(lane) {
		return true
	}
	if lane != "" && (agentWorkProjectLaneBlocksRoleSignalRouting(lane) || agentWorkProjectLaneIsReview(lane) || projectLaneRequiresImplementationGate(lane)) {
		return false
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return false
	}
	if agentWorkTextContainsAny(agentWorkTaskSearchText(task), []string{
		"synthesis",
		"synthesize",
		"summarize",
		"summary",
		"final",
		"report",
		"handoff",
		"assemble",
		"evidence pack",
		"decision",
		"documentation",
		"docs",
	}) {
		return true
	}
	return agentWorkTextContainsAny(agentWorkTaskTagText(task), []string{
		"synthesis",
		"synthesizer",
		"summary",
		"handoff",
		"report",
		"docs",
	})
}

func agentWorkTaskLooksValidationScoped(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if agentWorkTaskStructuredProjectEvidenceWork(task) && agentWorkTaskRequirementsContainWorkMode(task, "validation") {
		return true
	}
	if agentWorkTaskIsProactiveMetacognition(task) {
		return false
	}
	if agentWorkTaskLooksActionRouteValidationScoped(task) {
		return true
	}
	if agentWorkProjectLaneIsValidation(lane) {
		return true
	}
	if lane != "" && (agentWorkProjectLaneBlocksRoleSignalRouting(lane) || projectLaneRequiresImplementationGate(lane)) {
		return false
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return false
	}
	if agentWorkTextContainsAny(agentWorkTaskSearchText(task), []string{
		"validate",
		"validation",
		"browser",
		"smoke",
		"launch",
		"export",
		"acceptance",
	}) {
		return true
	}
	if agentWorkTextContainsAny(agentWorkTaskTagText(task), []string{
		"validation",
		"verify",
		"verification",
		"qa",
		"smoke",
		"browser-smoke",
		"acceptance",
	}) {
		return true
	}
	return agentWorkTextContainsAny(agentWorkTaskDescriptionText(task), []string{
		"browser smoke",
		"smoke test",
		"acceptance evidence",
		"verification pass",
		"validate output",
		"verify output",
	})
}

func agentWorkTaskLooksActionRouteValidationScoped(task WorkspaceTaskRecord) bool {
	text := strings.Join([]string{
		agentWorkTaskSearchText(task),
		agentWorkTaskDescriptionText(task),
		agentWorkTaskTagText(task),
	}, " ")
	return agentWorkTextContainsAny(text, []string{
		"resolve routed agent action request",
		"routed agent action request",
		"personal action request",
		"browser_screenshot",
		"browser-screenshot",
		"screenshot_capture",
		"screenshot-capture",
		"browser_visual_probe",
		"browser-visual-probe",
		"visual probe",
		"visual-qa",
		"browser-smoke",
		"capability-browser",
	})
}

func agentWorkTaskLooksArtifactProducingActionRoute(task WorkspaceTaskRecord) bool {
	text := strings.Join([]string{
		agentWorkTaskSearchText(task),
		agentWorkTaskDescriptionText(task),
		agentWorkTaskTagText(task),
	}, " ")
	return agentWorkTextContainsAny(text, []string{
		"resolve routed agent action request",
		"routed agent action request",
		"personal action request",
	})
}

func agentWorkTaskLooksStrategyScoped(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if projectLaneRequiresImplementationGate(lane) || agentWorkProjectLaneIsReview(lane) || agentWorkProjectLaneIsSynthesis(lane) {
		return false
	}
	if agentWorkProjectLaneIsStrategy(lane) {
		return true
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return true
	}
	if agentWorkTextContainsAny(agentWorkTaskSearchText(task), []string{
		"strategy",
		"strategic",
		"planning",
		"project framing",
		"design doc",
		"requirements",
		"specification",
		"task decomposition",
		"decompose",
		"work breakdown",
		"coordination plan",
		"implementation plan",
	}) {
		return true
	}
	return agentWorkTextContainsAny(agentWorkTaskTagText(task), []string{
		"strategy",
		"strategist",
		"planning",
		"coordination",
		"metacognition",
	})
}

func agentWorkTaskLooksImplementationScoped(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if agentWorkProjectLaneIsStrategy(lane) || agentWorkProjectLaneIsReview(lane) || agentWorkProjectLaneIsSynthesis(lane) {
		return false
	}
	if projectLaneRequiresImplementationGate(lane) {
		return true
	}
	if agentWorkTaskLooksAutonomousProjectRoot(task) {
		return false
	}
	if agentWorkTextContainsAny(agentWorkTaskSearchText(task), []string{
		"build",
		"builder",
		"implement",
		"implementation",
		"code",
		"coding",
		"frontend",
		"front-end",
		"backend",
		"back-end",
		"api",
		"ui",
		"dashboard",
		"service",
		"cli",
		"script",
		"runnable",
		"artifact",
		"fix",
		"repair",
		"integration glue",
	}) {
		return true
	}
	if agentWorkTextContainsAny(agentWorkTaskTagText(task), []string{
		"implementation",
		"implementer",
		"builder",
		"frontend",
		"backend",
		"fullstack",
		"code",
		"fix",
		"repair",
	}) {
		return true
	}
	return agentWorkTextContainsAny(agentWorkTaskDescriptionText(task), []string{
		"implement",
		"build",
		"repair",
		"fix",
		"code",
		"frontend",
		"backend",
	})
}

func agentWorkTaskBypassesImplementationGateByStructuredContract(task WorkspaceTaskRecord) bool {
	for _, hint := range task.WriteScopeHints {
		if strings.TrimSpace(hint) != "" {
			return false
		}
	}
	return agentWorkTaskStructuredProjectEvidenceWork(task)
}

func agentWorkTaskStructuredProjectEvidenceWork(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !agentWorkTaskRequirementsContainWorkMode(task, "review", "synthesis", "validation") {
		return false
	}
	if agentWorkTaskHasTag(task,
		"docs",
		"documentation",
		"planning",
		"plan-review",
		"plan_review",
		"product-contract",
		"product_contract",
		"spec-fidelity",
		"spec_fidelity",
		"requirements",
	) {
		return true
	}
	if agentWorkStringSliceContainsAnyFold(agentWorkTaskRequirementsStringSlice(task, "preferred_skills", "required_skills"),
		"docs",
		"documentation",
		"planning",
		"spec-fidelity",
		"spec_fidelity",
		"requirements",
	) {
		return true
	}
	return false
}

func agentWorkTaskRequirementsContainWorkMode(task WorkspaceTaskRecord, modes ...string) bool {
	values := agentWorkTaskRequirementsStringSlice(task, "required_work_modes", "preferred_work_modes")
	if len(values) == 0 {
		return false
	}
	return agentWorkStringSliceContainsAnyFold(values, modes...)
}

func agentWorkStringSliceContainsAnyFold(values []string, wants ...string) bool {
	for _, want := range wants {
		if containsAgentWorkStringFold(values, want) {
			return true
		}
	}
	return false
}

func agentWorkTaskLooksOperatorSpecRoot(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) != "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "" && !agentWorkProjectLaneIsStrategy(lane) {
		return false
	}
	kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
	if kind != "" && kind != model.TaskKindCoordination {
		return false
	}
	if len(agentWorkTaskRequirementsStringSlice(task, "operator_spec_doc", "operator_spec_doc_key")) > 0 {
		return true
	}
	text := strings.Join([]string{
		agentWorkTaskSearchText(task),
		agentWorkTaskDescriptionText(task),
		agentWorkTaskTagText(task),
	}, " ")
	return agentWorkTextContainsAny(text, []string{
		"operator spec",
		"operator_spec",
		"operator-spec",
		"root task",
		"root coordination",
		"create project",
		"establish project",
		"decompose product",
		"task decomposition",
		"autonomous mvp",
		"deployment run",
		"coordinate agents",
		"coordinate builders",
	})
}

func agentWorkTaskLooksAutonomousProjectRoot(task WorkspaceTaskRecord) bool {
	if agentWorkTaskLooksOperatorSpecRoot(task) {
		return true
	}
	if strings.TrimSpace(task.ProjectLane) != "" {
		return false
	}
	kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
	templateName := strings.ToLower(strings.TrimSpace(task.TaskTemplate))
	if kind != model.TaskKindCoordination && templateName != model.TaskTemplateIntegration {
		return false
	}
	if kind == model.TaskKindCoordination && templateName == model.TaskTemplateProject {
		return true
	}
	text := agentWorkTaskSearchText(task)
	return agentWorkTextContainsAny(text, []string{
		"autonomous coordination",
		"one root task",
		"only one root task",
		"strategic agent",
		"establish the project",
		"design and test approach",
		"create any needed subtasks",
		"coordinate builders",
		"coordinate builders, reviewer",
		"derive their own acceptance criteria",
		"task decomposition",
	})
}

func agentWorkProjectLaneIsStrategy(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "strategy", "strategic", "coordination", "planning", "plan", "spec", "specification", "requirements", "design", "framing":
		return true
	default:
		return false
	}
}

func agentWorkProjectLaneBlocksRoleSignalRouting(lane string) bool {
	lane = strings.ToLower(strings.TrimSpace(lane))
	return lane != "coordination" && agentWorkProjectLaneIsStrategy(lane)
}

func agentWorkProjectLaneIsReview(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "review", "qa", "test", "testing", "verification", "validation", "acceptance", "quality", "quality-review", "ux", "ux-review", "usability", "accessibility", "a11y", "smoke", "browser-smoke":
		return true
	default:
		return false
	}
}

func agentWorkProjectLaneIsPatchQueueReview(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "review", "reviewer":
		return true
	default:
		return false
	}
}

func agentWorkProjectLaneIsValidation(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "qa", "test", "testing", "verification", "validation", "acceptance", "quality", "quality-review", "ux", "ux-review", "usability", "accessibility", "a11y", "smoke", "browser-smoke":
		return true
	default:
		return false
	}
}

func agentWorkProjectLaneIsSynthesis(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "synthesis", "summary", "summarization", "documentation", "docs", "handoff", "report", "final", "integration", "integrator":
		return true
	default:
		return false
	}
}

func agentWorkTaskSearchText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.TaskKind,
		task.TaskTemplate,
		task.TaskClass,
		task.ProjectLane,
	}, " "))
}

func agentWorkTaskDescriptionText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.TrimSpace(task.Description))
}

func agentWorkTaskTagText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.Join(task.Tags, " "))
}

func agentWorkTextContainsAny(text string, needles []string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" && strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func (s *Store) attachAgentWorkHydration(ctx context.Context, result *AgentWorkNextResult, filter AgentWorkNextFilter) error {
	attachAgentWorkProjectDigest(result)
	if result == nil || !filter.IncludeHydration || result.Task == nil {
		return nil
	}
	docKeys := enrichAgentWorkHydrationDocKeys(result.Task.TaskID, result.Task.ProjectID, filter.DocKeys)
	bundle, err := s.GetTaskHydrationBundle(ctx, TaskHydrationFilter{
		TaskID:           result.Task.TaskID,
		WorkspaceID:      result.WorkspaceID,
		DocKeys:          docKeys,
		IncludeAllDocs:   filter.IncludeAllDocs,
		UpdatesLimit:     filter.UpdatesLimit,
		ArtifactLimit:    filter.ArtifactLimit,
		RelatedTaskLimit: filter.RelatedTaskLimit,
	})
	if err != nil {
		return err
	}
	result.Hydration = &bundle
	return nil
}

func attachAgentWorkProjectDigest(result *AgentWorkNextResult) {
	if result == nil || result.Task == nil {
		return
	}
	attachAgentWorkResultTaskProjectDigest(result, *result.Task)
}

func attachAgentWorkResultTaskProjectDigest(result *AgentWorkNextResult, task WorkspaceTaskRecord) {
	if result == nil {
		return
	}
	result.ProjectID = strings.TrimSpace(task.ProjectID)
	result.TaskKind = strings.TrimSpace(task.TaskKind)
	result.ProjectLane = strings.TrimSpace(task.ProjectLane)
	result.RequiresProjectGate = projectTaskRequiresImplementationGate(task)
}

func (s *Store) attachAgentWorkPacket(ctx context.Context, result *AgentWorkNextResult, filter AgentWorkNextFilter) {
	if result == nil || !filter.IncludePacket {
		return
	}
	if !result.HasWork {
		if result.Reason == "profile_gate_closed" {
			result.Packet = &AgentWorkPacket{
				WorkType:            "profile_gate_closed",
				CoordinationState:   "profile_gate_closed",
				PreferredTransition: "agent_profile_update",
				WhyNow:              firstNonEmpty(strings.TrimSpace(result.ProfileGateReason), "profile_disallows_autonomous_execution"),
				Gate: &AgentWorkGatePacket{
					GateState:  "closed",
					GateType:   "profile_autonomous_execution",
					NeededFrom: "agent.profile.update",
					Summary:    firstNonEmpty(strings.TrimSpace(result.ProfileGateSummary), "Agent profile disallows autonomous work selection."),
				},
			}
		}
		return
	}
	packet := AgentWorkPacket{
		WorkType:            strings.TrimSpace(result.Reason),
		CoordinationState:   coordinationStateForAgentWork(result),
		PreferredTransition: preferredTransitionForAgentWork(result),
		WhyNow:              whyNowForAgentWork(result),
		ContextHints:        contextHintsForAgentWork(result),
	}
	if result.Task != nil {
		packet.ProjectID = strings.TrimSpace(result.Task.ProjectID)
		packet.TaskKind = strings.TrimSpace(result.Task.TaskKind)
		packet.ProjectLane = strings.TrimSpace(result.Task.ProjectLane)
		packet.RequiresProjectGate = projectTaskRequiresImplementationGate(*result.Task)
		if projectRoleScopeTask(*result.Task) && packet.Gate == nil {
			packet.Gate = &AgentWorkGatePacket{
				GateState:  "open",
				GateType:   "project_role_scope_authority_transition",
				NeededFrom: "project_role_assign",
				Summary:    "project role/scope authority-transition task requires project_role_assign; chat/status approval is not a transition",
			}
		}
		if projectClaimRepairTask(*result.Task) && packet.Gate == nil {
			packet.Gate = &AgentWorkGatePacket{
				GateState:  "open",
				GateType:   "project_claim_repair_authority_transition",
				NeededFrom: "project_claim_repair_receipt",
				Summary:    "project claim repair task must produce a durable repair receipt via project_role_assign, project_patch_queue_followup, task_submit, or typed denial/blocker; status/delegation chatter is not a transition",
			}
		}
		if recovery, ok := agentWorkABPCRecoveryAction(*result.Task); ok {
			packet.WorkType = "abpc_side_effect_recovery_action"
			packet.CoordinationState = "side_effect_resolution_successor"
			packet.PreferredTransition = recovery.PreferredTransition
			packet.WhyNow = recovery.Summary
			packet.HandoffToAgentID = firstNonEmpty(recovery.TargetAgentID, recovery.OwnerAgentID)
			packet.Gate = &AgentWorkGatePacket{
				GateState:  "open",
				GateType:   "abpc_recovery_action",
				NeededFrom: firstNonEmpty(recovery.PreferredTransition, "side_effect_resolution_successor"),
				Summary:    recovery.Summary,
			}
		}
		s.attachProjectCoordinationToAgentWork(ctx, result, result.Task, &packet)
	}
	if result.Session != nil {
		packet.Resume = &AgentWorkResumePacket{
			SessionID: strings.TrimSpace(result.Session.SessionID),
			Summary:   strings.TrimSpace(result.ResumeSummary),
			UpdatedAt: strings.TrimSpace(result.Session.UpdatedAt),
		}
		if len(result.Session.BlockedOn) > 0 {
			packet.Blockers = append([]model.AgentUpdateBlockedRef(nil), result.Session.BlockedOn...)
		}
		if strings.TrimSpace(result.Session.DecisionNeededFrom) != "" || strings.TrimSpace(result.Session.DecisionType) != "" {
			packet.Decision = &AgentWorkDecisionPacket{
				NeededFrom:   strings.TrimSpace(result.Session.DecisionNeededFrom),
				DecisionType: strings.TrimSpace(result.Session.DecisionType),
			}
			packet.Gate = &AgentWorkGatePacket{
				GateState:  "open",
				GateType:   firstNonEmpty(strings.TrimSpace(result.Session.DecisionType), "decision"),
				NeededFrom: strings.TrimSpace(result.Session.DecisionNeededFrom),
				Summary:    firstNonEmpty(strings.TrimSpace(result.ResumeSummary), strings.TrimSpace(result.Session.Summary)),
			}
		}
		packet.HandoffToAgentID = strings.TrimSpace(result.Session.HandoffTo)
		switch strings.ToUpper(strings.TrimSpace(result.Session.Status)) {
		case model.SessionStatusBlocked:
			packet.Unblock = &AgentWorkUnblockPacket{
				UnblockState: unblockStateForAgentWork(result),
				Trigger:      strings.TrimSpace(result.Trigger),
				BlockerKinds: blockerKindsForAgentWork(result.Session.BlockedOn),
				Summary:      firstNonEmpty(strings.TrimSpace(result.ResumeSummary), strings.TrimSpace(result.Session.Summary)),
			}
		case model.SessionStatusHandoffPending:
			packet.Handoff = &AgentWorkHandoffPacket{
				HandoffState: handoffStateForAgentWork(result),
				ToAgentID:    strings.TrimSpace(result.Session.HandoffTo),
				Summary:      firstNonEmpty(strings.TrimSpace(result.ResumeSummary), strings.TrimSpace(result.Session.Summary)),
			}
		}
	}
	if filter.IncludeAdvisory {
		if advisory, ok := s.buildAgentWorkAdvisory(ctx, result, filter); ok {
			packet.Advisory = &advisory
		}
	}
	if packet.Resume == nil {
		packet.Resume = nil
	}
	if packet.Decision == nil {
		packet.Decision = nil
	}
	result.Packet = &packet
}

func (s *Store) attachProjectCoordinationToAgentWork(ctx context.Context, result *AgentWorkNextResult, task *WorkspaceTaskRecord, packet *AgentWorkPacket) {
	if s == nil || result == nil || task == nil {
		return
	}
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		return
	}
	workspaceID := strings.TrimSpace(result.WorkspaceID)
	if workspaceID == "" {
		return
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return
	}
	raw, err := json.Marshal(coordination)
	if err != nil {
		return
	}
	result.ProjectCoordination = append(result.ProjectCoordination[:0], raw...)
	if packet != nil {
		packet.ProjectCoordination = append(packet.ProjectCoordination[:0], raw...)
	}
}

func (s *Store) projectImplementationGateClosed(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" || !projectTaskRequiresImplementationGate(task) {
		return nil, false, nil
	}
	status, err := s.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		return nil, false, err
	}
	if !status.ImplementationReady {
		return projectImplementationGateClosedPacket(task, projectID, status.OverallState, "project.gates.status", projectImplementationGateClosedSummary(status)), true, nil
	}
	if !projectPhaseAllowsImplementationWork(status.CurrentPhase) {
		return projectImplementationGateClosedPacket(task, projectID, ProjectGateStateBlocked, "project.gates.status", "implementation_phase_open: project phase is "+strings.ToLower(strings.TrimSpace(status.CurrentPhase))), true, nil
	}
	if _, ok, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID); err != nil {
		return nil, false, err
	} else if !ok {
		return projectImplementationGateClosedPacket(task, projectID, ProjectGateStateBlocked, "project.gates.status", "strategic_lead_active: active strategic lead lease is required before implementation work"), true, nil
	}
	return nil, false, nil
}

func (s *Store) projectValidationArtifactGateClosed(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" || !agentWorkTaskLooksValidationScoped(task) {
		return nil, false, nil
	}
	if agentWorkTaskLooksArtifactProducingActionRoute(task) {
		return nil, false, nil
	}
	if agentWorkTaskLooksABPCRecoveryAction(task) {
		return nil, false, nil
	}
	if isTerminalTaskStatus(task.Status) {
		return nil, false, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return nil, false, err
	}
	artifactAvailable, err := s.projectValidationReviewableArtifactAvailableForTask(ctx, workspaceID, coordination, task)
	if err != nil {
		return nil, false, err
	}
	if artifactAvailable {
		return nil, false, nil
	}
	return projectValidationArtifactMissingPacket(task, projectID), true, nil
}

func (s *Store) projectValidationReviewableArtifactAvailableForTask(ctx context.Context, workspaceID string, coordination ProjectCoordinationRecord, task WorkspaceTaskRecord) (bool, error) {
	refs, err := s.projectValidationArtifactRefsFromTaskAndDocs(ctx, workspaceID, task)
	if err != nil {
		return false, err
	}
	if refs.hasSpecificTarget() {
		return projectValidationTargetedReviewableArtifactAvailable(coordination, refs), nil
	}
	if refs.hasRunnableSurface() {
		return true, nil
	}
	return projectValidationReviewableArtifactAvailable(coordination), nil
}

func projectValidationReviewableArtifactAvailable(coordination ProjectCoordinationRecord) bool {
	for _, branch := range coordination.Branches {
		if projectValidationBranchReviewable(branch) {
			return true
		}
	}
	for _, item := range coordination.PatchQueueItems {
		if projectValidationPatchQueueItemReviewable(item) {
			return true
		}
	}
	return false
}

func projectValidationTargetedReviewableArtifactAvailable(coordination ProjectCoordinationRecord, refs projectValidationArtifactRefs) bool {
	if refs.hasPatchQueueTarget() {
		return projectValidationTargetedPatchQueueArtifactAvailable(coordination, refs)
	}
	for _, branch := range coordination.Branches {
		if !projectValidationBranchReviewable(branch) {
			continue
		}
		if len(refs.branchIDs) > 0 && containsAgentWorkStringFold(refs.branchIDs, branch.BranchID) {
			return true
		}
	}
	for _, item := range coordination.PatchQueueItems {
		if !projectValidationPatchQueueItemReviewable(item) {
			continue
		}
		if len(refs.branchIDs) > 0 && !containsAgentWorkStringFold(refs.branchIDs, item.BranchID) {
			continue
		}
		if len(refs.queueIDs) > 0 && !containsAgentWorkStringFold(refs.queueIDs, item.QueueID) {
			continue
		}
		if len(refs.itemIDs) > 0 && !containsAgentWorkStringFold(refs.itemIDs, item.ItemID) {
			continue
		}
		return true
	}
	return false
}

func projectValidationTargetedPatchQueueArtifactAvailable(coordination ProjectCoordinationRecord, refs projectValidationArtifactRefs) bool {
	for _, item := range coordination.PatchQueueItems {
		if !projectValidationPatchQueueItemReviewable(item) {
			continue
		}
		if len(refs.branchIDs) > 0 && !containsAgentWorkStringFold(refs.branchIDs, item.BranchID) {
			continue
		}
		if len(refs.queueIDs) > 0 && !containsAgentWorkStringFold(refs.queueIDs, item.QueueID) {
			continue
		}
		if len(refs.itemIDs) > 0 && !containsAgentWorkStringFold(refs.itemIDs, item.ItemID) {
			continue
		}
		return true
	}
	return false
}

func projectValidationBranchReviewable(branch ProjectBranchRecord) bool {
	return strings.EqualFold(strings.TrimSpace(branch.Status), ProjectBranchStatusReadyForReview) &&
		strings.TrimSpace(branch.HeadSHA) != "" &&
		strings.TrimSpace(branch.ReviewDocKey) != ""
}

func projectValidationPatchQueueItemReviewable(item ProjectPatchQueueItemRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(item.State)) {
	case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed, ProjectPatchQueueStateBlocked, ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated:
		return strings.TrimSpace(item.BranchID) != "" &&
			strings.TrimSpace(item.HeadSHA) != "" &&
			strings.TrimSpace(firstNonEmpty(item.ReviewDocKey, item.EvidenceDocKey)) != ""
	default:
		return false
	}
}

type projectValidationArtifactRefs struct {
	branchIDs    []string
	queueIDs     []string
	itemIDs      []string
	runnableURLs []string
}

func (refs projectValidationArtifactRefs) hasSpecificTarget() bool {
	return len(refs.branchIDs) > 0 || len(refs.queueIDs) > 0 || len(refs.itemIDs) > 0
}

func (refs projectValidationArtifactRefs) hasPatchQueueTarget() bool {
	return len(refs.queueIDs) > 0 || len(refs.itemIDs) > 0
}

func (refs projectValidationArtifactRefs) hasRunnableSurface() bool {
	return len(refs.runnableURLs) > 0
}

var (
	projectValidationArtifactRefPattern        = regexp.MustCompile(`(?i)\b(branch_id|branch id|target_branch_id|queue_id|queue id|patch_queue_id|item_id|item id|patch_item_id|runnable_url|runnable url|surface_url|surface url|preview_url|preview url)\b\s*[:=]\s*([^\s,\]\)]+)`)
	projectValidationArtifactPatchQueuePattern = regexp.MustCompile(`(?i)\bpatch\s+queue\s*:\s*([A-Za-z0-9._:-]+)/([A-Za-z0-9._:-]+)`)
	projectValidationEvidenceRefPattern        = regexp.MustCompile(`(?i)\b(branch|patch_queue|queue|patch_item|item):([A-Za-z0-9._:-]+)`)
	projectValidationRunnableURLPattern        = regexp.MustCompile("(?i)\\bhttps?://[^\\s<>\\]\\)}\\x60'\\\"]+")
	projectValidationDocRefPattern             = regexp.MustCompile(`(?i)\b(promotion_doc|promotion doc|doc_key|doc key|evidence_doc_key|evidence doc key|review_doc_key|review doc key|visual_doc_key|visual doc key)\b\s*[:=]\s*([^\s,\]\)]+)`)
	projectValidationInlineDocRefPattern       = regexp.MustCompile(`(?i)\b(?:doc|workspace_doc):([A-Za-z0-9._:/-]+)`)
)

func (s *Store) projectValidationArtifactRefsFromTaskAndDocs(ctx context.Context, workspaceID string, task WorkspaceTaskRecord) (projectValidationArtifactRefs, error) {
	refs := projectValidationArtifactRefsFromTask(task)
	docKeys := projectValidationDocKeysFromTask(task)
	seen := make(map[string]bool, len(docKeys))
	for depth := 0; depth < 2 && len(docKeys) > 0; depth++ {
		nextDocKeys := []string{}
		for _, docKey := range docKeys {
			docKey = projectValidationCleanDocKey(docKey)
			if docKey == "" || seen[strings.ToLower(docKey)] {
				continue
			}
			seen[strings.ToLower(docKey)] = true
			doc, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
			if err != nil {
				if projectValidationWorkspaceDocNotFound(err) {
					continue
				}
				return refs, err
			}
			docText := strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n")
			refs = projectValidationArtifactRefsFromText(refs, docText)
			nextDocKeys = append(nextDocKeys, projectValidationDocKeysFromText(docText)...)
		}
		docKeys = nextDocKeys
	}
	return projectValidationNormalizeArtifactRefs(refs), nil
}

func projectValidationArtifactRefsFromTask(task WorkspaceTaskRecord) projectValidationArtifactRefs {
	refs := projectValidationArtifactRefs{
		branchIDs: append([]string(nil), agentWorkTaskRequirementsStringSlice(task,
			"branch_id", "branch_ids", "target_branch_id", "target_branch_ids")...),
		queueIDs: append([]string(nil), agentWorkTaskRequirementsStringSlice(task,
			"queue_id", "queue_ids", "patch_queue_id", "patch_queue_ids")...),
		itemIDs: append([]string(nil), agentWorkTaskRequirementsStringSlice(task,
			"item_id", "item_ids", "patch_item_id", "patch_item_ids")...),
		runnableURLs: append([]string(nil), agentWorkTaskRequirementsStringSlice(task,
			"runnable_url", "runnable_urls", "surface_url", "surface_urls", "preview_url", "preview_urls")...),
	}
	text := strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
		task.TaskRequirementsJSON,
	}, "\n")
	refs = projectValidationArtifactRefsFromText(refs, text)
	return projectValidationNormalizeArtifactRefs(refs)
}

func projectValidationArtifactRefsFromText(refs projectValidationArtifactRefs, text string) projectValidationArtifactRefs {
	for _, match := range projectValidationArtifactRefPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		key := agentWorkNormalizeSemanticSignal(match[1])
		value := projectValidationCleanArtifactRef(match[2])
		if value == "" {
			continue
		}
		switch key {
		case "branch id", "target branch id":
			refs.branchIDs = append(refs.branchIDs, value)
		case "queue id", "patch queue id":
			refs.queueIDs = append(refs.queueIDs, value)
		case "item id", "patch item id":
			refs.itemIDs = append(refs.itemIDs, value)
		case "runnable url", "surface url", "preview url":
			refs.runnableURLs = append(refs.runnableURLs, value)
		}
	}
	for _, match := range projectValidationArtifactPatchQueuePattern.FindAllStringSubmatch(text, -1) {
		if len(match) >= 3 {
			refs.queueIDs = append(refs.queueIDs, projectValidationCleanArtifactRef(match[1]))
			refs.itemIDs = append(refs.itemIDs, projectValidationCleanArtifactRef(match[2]))
		}
	}
	for _, match := range projectValidationEvidenceRefPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(match[1]))
		value := projectValidationCleanArtifactRef(match[2])
		if value == "" {
			continue
		}
		switch key {
		case "branch":
			refs.branchIDs = append(refs.branchIDs, value)
		case "patch_queue", "queue":
			refs.queueIDs = append(refs.queueIDs, value)
		case "patch_item", "item":
			refs.itemIDs = append(refs.itemIDs, value)
		}
	}
	for _, match := range projectValidationRunnableURLPattern.FindAllString(text, -1) {
		refs.runnableURLs = append(refs.runnableURLs, match)
	}
	return refs
}

func projectValidationNormalizeArtifactRefs(refs projectValidationArtifactRefs) projectValidationArtifactRefs {
	refs.branchIDs = projectValidationCleanArtifactRefs(refs.branchIDs)
	refs.queueIDs = projectValidationCleanArtifactRefs(refs.queueIDs)
	refs.itemIDs = projectValidationCleanArtifactRefs(refs.itemIDs)
	refs.runnableURLs = projectValidationCleanRunnableURLs(refs.runnableURLs)
	return refs
}

func projectValidationDocKeysFromTask(task WorkspaceTaskRecord) []string {
	text := strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
		task.TaskRequirementsJSON,
	}, "\n")
	return projectValidationDocKeysFromText(text)
}

func projectValidationDocKeysFromText(text string) []string {
	out := []string{}
	for _, match := range projectValidationDocRefPattern.FindAllStringSubmatch(text, -1) {
		if len(match) >= 3 {
			out = append(out, projectValidationCleanDocKey(match[2]))
		}
	}
	for _, match := range projectValidationInlineDocRefPattern.FindAllStringSubmatch(text, -1) {
		if len(match) >= 2 {
			out = append(out, projectValidationCleanDocKey(match[1]))
		}
	}
	return uniqueTrimmedAgentWork(out)
}

func projectValidationCleanDocKey(value string) string {
	value = projectValidationCleanArtifactRef(value)
	lower := strings.ToLower(value)
	for _, prefix := range []string{"workspace_doc:", "doc:"} {
		if strings.HasPrefix(lower, prefix) {
			value = value[len(prefix):]
			break
		}
	}
	return projectValidationCleanArtifactRef(value)
}

func projectValidationWorkspaceDocNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "workspace doc not found:")
}

func projectValidationCleanArtifactRef(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`'\"[](){}<>.,;")
}

func projectValidationCleanArtifactRefs(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if cleaned := projectValidationCleanArtifactRef(value); cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return uniqueTrimmedAgentWork(out)
}

func projectValidationCleanRunnableURLs(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		cleaned := projectValidationCleanArtifactRef(value)
		if projectValidationRunnableSurfaceURLValid(cleaned) {
			out = append(out, cleaned)
		}
	}
	return uniqueTrimmedAgentWork(out)
}

func projectValidationRunnableSurfaceURLValid(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	switch strings.ToLower(value) {
	case "missing", "none", "null", "nil", "n/a", "na", "tbd", "todo", "unknown", "placeholder", "example":
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	return (scheme == "http" || scheme == "https") && strings.TrimSpace(parsed.Host) != ""
}

func (s *Store) projectImplementationFreshClaimRequiresTargetedSwitch(ctx context.Context, workspaceID, agentID string, task WorkspaceTaskRecord) (*AgentWorkPacket, bool, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" || !projectTaskRequiresImplementationGate(task) {
		return nil, false, nil
	}
	if !projectLaneRequiresImplementationGate(task.ProjectLane) {
		return nil, false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
		return nil, false, nil
	}
	if task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
		return nil, false, nil
	}
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return nil, false, err
	}
	if agentWorkImplementationRoleSupportsTaskClaim(coordination, agentID, task) {
		return nil, false, nil
	}
	if agentOwnsProjectTaskBranchEvidence(agentID, task, coordination) {
		return nil, false, nil
	}
	for _, role := range coordination.Roles {
		if !strings.EqualFold(strings.TrimSpace(role.Status), ProjectRoleStatusActive) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(role.RoleType), ProjectRoleImplementer) {
			continue
		}
		if len(writeScopePaths(role.WriteScopeJSON)) == 0 {
			continue
		}
		summary := "project implementation task has scoped implementer roles; fresh claims require targeted runtime_switch_task delegation"
		return projectImplementationTargetedDelegationPacket(task, projectID, summary), true, nil
	}
	return nil, false, nil
}

func agentOwnsProjectTaskBranchEvidence(agentID string, task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) bool {
	agentID = strings.TrimSpace(agentID)
	taskID := strings.TrimSpace(task.TaskID)
	claimBranchID := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimBranchID))
	if agentID == "" || taskID == "" {
		return false
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(branch.Status))
		if claimBranchID != "" && strings.TrimSpace(branch.BranchID) == claimBranchID {
			if status == ProjectBranchStatusReadyForReview {
				return strings.TrimSpace(branch.HeadSHA) != "" && strings.TrimSpace(branch.ReviewDocKey) != ""
			}
			switch status {
			case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked:
				return true
			}
		}
		if agentWorkPatchQueueRevisionFollowupReferencesBranch(task, branch.BranchID) {
			return true
		}
		if strings.TrimSpace(branch.ActiveTaskID) == taskID || strings.TrimSpace(branch.ActiveClaimID) == taskID {
			switch status {
			case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked:
				return true
			}
		}
	}
	return false
}

func agentWorkReservedNonImplementationBranchDoesNotOwnWriteScope(branch ProjectBranchRecord, tasks []WorkspaceTaskRecord) bool {
	if strings.ToUpper(strings.TrimSpace(branch.Status)) != ProjectBranchStatusReserved {
		return false
	}
	if strings.TrimSpace(branch.HeadSHA) != "" || strings.TrimSpace(branch.ReviewDocKey) != "" {
		return false
	}
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	if activeTaskID == "" {
		return false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) != activeTaskID {
			continue
		}
		return !projectTaskRequiresImplementationGate(task)
	}
	return false
}

func agentWorkSameOwnerReadyBranchFollowupTask(task WorkspaceTaskRecord, branch ProjectBranchRecord) bool {
	if strings.ToUpper(strings.TrimSpace(branch.Status)) != ProjectBranchStatusReadyForReview {
		return false
	}
	return strings.TrimSpace(workspaceTaskPointerValue(task.ClaimBranchID)) == strings.TrimSpace(branch.BranchID) &&
		strings.TrimSpace(workspaceTaskPointerValue(task.ClaimAgentID)) == strings.TrimSpace(branch.AgentID)
}

func projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) bool {
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
		return false
	}
	return projectPatchQueueItemsReleaseBranchWriteScope(branchID, headSHA, items)
}

func agentWorkRevisionPredecessorBranchReleasedByNewerSource(task WorkspaceTaskRecord, branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) bool {
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return false
	}
	if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
		return false
	}
	source, ok := agentWorkPatchQueueRevisionSourceItem(task, items)
	if !ok {
		return false
	}
	branchID := strings.TrimSpace(branch.BranchID)
	if branchID == "" || strings.TrimSpace(source.BranchID) == branchID {
		return false
	}
	predecessor, ok := agentWorkPatchQueueTerminalItemForBranchHead(branch, items)
	if !ok {
		return false
	}
	return agentWorkPatchQueueItemDecidedAfter(source, predecessor)
}

func agentWorkPatchQueueRevisionSourceItem(task WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord) (ProjectPatchQueueItemRecord, bool) {
	var best ProjectPatchQueueItemRecord
	ok := false
	for _, item := range items {
		if !agentWorkPatchQueueRevisionFollowupMatchesItem(task, item) {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected, ProjectPatchQueueStateCanceled, ProjectPatchQueueStateIntegrated:
		default:
			continue
		}
		if !ok || agentWorkPatchQueueItemDecidedAfter(item, best) {
			best = item
			ok = true
		}
	}
	return best, ok
}

func agentWorkPatchQueueTerminalItemForBranchHead(branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) (ProjectPatchQueueItemRecord, bool) {
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	if branchID == "" || headSHA == "" {
		return ProjectPatchQueueItemRecord{}, false
	}
	var best ProjectPatchQueueItemRecord
	ok := false
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID || strings.TrimSpace(item.HeadSHA) != headSHA {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateIntegrated, ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected, ProjectPatchQueueStateCanceled:
		default:
			continue
		}
		if !ok || agentWorkPatchQueueItemDecidedAfter(item, best) {
			best = item
			ok = true
		}
	}
	return best, ok
}

func projectPatchQueueItemsReleaseBranchWriteScope(branchID, headSHA string, items []ProjectPatchQueueItemRecord) bool {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	hasTerminalDecisionForHead := false
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case ProjectPatchQueueStateProposed, ProjectPatchQueueStateClaimed:
			return false
		case ProjectPatchQueueStateAccepted:
			if strings.TrimSpace(item.HeadSHA) == headSHA {
				return false
			}
		case ProjectPatchQueueStateBlocked, ProjectPatchQueueStateRejected, ProjectPatchQueueStateCanceled, ProjectPatchQueueStateIntegrated:
			if strings.TrimSpace(item.HeadSHA) == headSHA {
				hasTerminalDecisionForHead = true
			}
		}
	}
	return hasTerminalDecisionForHead
}

func agentWorkEffectiveClaimWriteScopeJSON(other WorkspaceTaskRecord, branches []ProjectBranchRecord, repoID string) string {
	rawScopeJSON := strings.TrimSpace(workspaceTaskPointerValue(other.ClaimWriteScopeJSON))
	branchID := strings.TrimSpace(workspaceTaskPointerValue(other.ClaimBranchID))
	repoID = strings.TrimSpace(repoID)
	if branchID == "" || repoID == "" {
		return rawScopeJSON
	}
	for _, branch := range branches {
		if !agentWorkBranchMatchesActiveClaim(branch, other, repoID, branchID) {
			continue
		}
		scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
		if len(writeScopePaths(scopeJSON)) == 0 {
			return rawScopeJSON
		}
		if !writeScopePathsSameSet(writeScopePaths(scopeJSON), writeScopePaths(rawScopeJSON)) {
			return rawScopeJSON
		}
		return scopeJSON
	}
	return rawScopeJSON
}

func agentWorkBranchMatchesActiveClaim(branch ProjectBranchRecord, other WorkspaceTaskRecord, repoID, branchID string) bool {
	if strings.TrimSpace(branch.BranchID) != branchID || strings.TrimSpace(branch.RepoID) != repoID {
		return false
	}
	if !projectBranchOwnsWriteScope(branch) {
		return false
	}
	claimAgentID := strings.TrimSpace(workspaceTaskPointerValue(other.ClaimAgentID))
	if branchAgentID := strings.TrimSpace(branch.AgentID); claimAgentID != "" && branchAgentID != "" && branchAgentID != claimAgentID {
		return false
	}
	taskID := strings.TrimSpace(other.TaskID)
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	return activeTaskID == taskID || activeClaimID == taskID
}

func agentWorkImplementationClaimRepository(coordination ProjectCoordinationRecord) (ProjectRepositoryRecord, bool) {
	for _, repo := range coordination.Repositories {
		if repo.IsCanonical && strings.EqualFold(strings.TrimSpace(repo.RepoStatus), ProjectRepositoryStatusReady) && strings.TrimSpace(repo.RemoteURL) != "" {
			return repo, true
		}
	}
	for _, repo := range coordination.Repositories {
		if strings.EqualFold(strings.TrimSpace(repo.RepoStatus), ProjectRepositoryStatusReady) && strings.TrimSpace(repo.RemoteURL) != "" {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func agentWorkImplementationClaimWriteScope(coordination ProjectCoordinationRecord, agentID string) (string, bool) {
	role, ok := agentWorkImplementationClaimRole(coordination, agentID)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(role.WriteScopeJSON), true
}

func agentWorkImplementationClaimRole(coordination ProjectCoordinationRecord, agentID string) (ProjectRoleRecord, bool) {
	agentID = strings.TrimSpace(agentID)
	for _, role := range coordination.Roles {
		if strings.TrimSpace(role.AgentID) != agentID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(role.Status), ProjectRoleStatusActive) || !strings.EqualFold(strings.TrimSpace(role.RoleType), ProjectRoleImplementer) {
			continue
		}
		writeScopeJSON := strings.TrimSpace(role.WriteScopeJSON)
		if len(writeScopePaths(writeScopeJSON)) > 0 {
			return role, true
		}
	}
	return ProjectRoleRecord{}, false
}

func agentWorkImplementationRoleCoversTask(coordination ProjectCoordinationRecord, agentID string, task WorkspaceTaskRecord) bool {
	role, ok := agentWorkImplementationClaimRole(coordination, agentID)
	if !ok {
		return false
	}
	taskWriteScopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		return false
	}
	taskPaths := writeScopePaths(taskWriteScopeJSON)
	rolePaths := writeScopePaths(role.WriteScopeJSON)
	return writeScopePathsCoveredBy(taskPaths, rolePaths)
}

func agentWorkImplementationRoleSupportsTaskClaim(coordination ProjectCoordinationRecord, agentID string, task WorkspaceTaskRecord) bool {
	role, ok := agentWorkImplementationClaimRole(coordination, agentID)
	if !ok {
		return false
	}
	taskWriteScopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		return false
	}
	taskPaths := writeScopePaths(taskWriteScopeJSON)
	rolePaths := writeScopePaths(role.WriteScopeJSON)
	if writeScopePathsCoveredBy(taskPaths, rolePaths) {
		return true
	}
	return agentWorkShouldPreferRoleScopeForTrustFirstTask(task, taskWriteScopeJSON, role)
}

func agentWorkRevisionSourceBranchWriteScope(coordination ProjectCoordinationRecord, task WorkspaceTaskRecord, repoID, agentID string) (string, bool) {
	if !agentWorkPatchQueueRevisionFollowupTask(task) {
		return "", false
	}
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	for _, branch := range coordination.Branches {
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if agentID == "" || strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if !agentWorkPatchQueueRevisionFollowupReferencesBranch(task, branch.BranchID) {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked, ProjectBranchStatusReadyForReview:
		default:
			continue
		}
		scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
		if len(writeScopePaths(scopeJSON)) == 0 {
			continue
		}
		return scopeJSON, true
	}
	return "", false
}

func agentWorkImplementationTaskClaimWriteScope(task WorkspaceTaskRecord) (string, bool) {
	paths := normalizeStringSlice(task.WriteScopeHints)
	explicitPaths := len(paths) > 0
	if len(paths) == 0 {
		paths = agentWorkTaskRequirementWriteScopeHints(task.TaskRequirementsJSON)
		explicitPaths = len(paths) > 0
	}
	if len(paths) == 0 && strings.EqualFold(strings.TrimSpace(agentWorkTaskRequirementString(task, "patch_queue_task_kind")), "revision") {
		paths = normalizeStringSlice(agentWorkTaskRequirementsStringSlice(task, "candidate_pathset"))
	}
	if len(paths) == 0 {
		paths = agentWorkSemanticTaskWriteScopeHintsWithoutExplicitHints(task)
	}
	if len(paths) == 0 {
		return "", false
	}
	if explicitPaths && agentWorkTaskPreservesExplicitWriteScopeHints(task) {
		return agentWorkTaskWriteScopeJSON(paths)
	}
	if narrowed := agentWorkSemanticTaskWriteScopeHints(task, paths); len(narrowed) > 0 {
		paths = narrowed
	}
	return agentWorkTaskWriteScopeJSON(paths)
}

func agentWorkTaskWriteScopeJSON(paths []string) (string, bool) {
	raw, err := json.Marshal(map[string][]string{"paths": paths})
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func agentWorkTaskPreservesExplicitWriteScopeHints(task WorkspaceTaskRecord) bool {
	if agentWorkTaskRequirementBool(task, "preserve_write_scope_hints", "write_scope_hints_authoritative") {
		return true
	}
	if strings.EqualFold(agentWorkTaskRequirementString(task, "admission_kind"), "abpc_recovery_action") ||
		strings.EqualFold(agentWorkTaskRequirementString(task, "schema"), "artifact_bound_side_effect_resolution_followup.v1") ||
		agentWorkTaskRequirementBool(task, "product_first_root") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(agentWorkTaskRequirementString(task, "product_slice", "task_slice"))) {
	case "acceptance_tests", "acceptance-test-matrix", "acceptance_test_matrix", "full_acceptance", "cold_verification":
		return true
	default:
		return false
	}
}

func agentWorkShouldPreferRoleScopeForTrustFirstTask(task WorkspaceTaskRecord, taskWriteScopeJSON string, role ProjectRoleRecord) bool {
	roleWriteScopeJSON := strings.TrimSpace(role.WriteScopeJSON)
	taskPaths := writeScopePaths(taskWriteScopeJSON)
	rolePaths := writeScopePaths(roleWriteScopeJSON)
	if len(taskPaths) == 0 || len(rolePaths) == 0 {
		return false
	}
	if agentWorkWriteScopeJSONIsBroad(roleWriteScopeJSON) {
		return false
	}
	if !agentWorkScopeOverrideAnchored(taskPaths, rolePaths) {
		return false
	}
	if agentWorkLooksBoundaryTransitionRole(role) {
		return true
	}
	if !agentWorkWriteScopeLooksCandidateWide(taskPaths) {
		return false
	}
	if agentWorkRevisionTaskScopeRepairShape(task) {
		return true
	}
	return agentWorkLooksScopeRepairRole(role)
}

func agentWorkScopeOverrideAnchored(taskPaths, overridePaths []string) bool {
	if len(taskPaths) == 0 || len(overridePaths) == 0 {
		return false
	}
	return writeScopePathsCoveredBy(taskPaths, overridePaths) ||
		writeScopePathsCoveredBy(overridePaths, taskPaths) ||
		writeScopesOverlap(taskPaths, overridePaths)
}

func agentWorkShouldPreferRoleScopeForRevision(task WorkspaceTaskRecord, taskWriteScopeJSON, roleWriteScopeJSON string) bool {
	return agentWorkShouldPreferRoleScopeForTrustFirstTask(task, taskWriteScopeJSON, ProjectRoleRecord{
		RoleType:       ProjectRoleImplementer,
		Status:         ProjectRoleStatusActive,
		WriteScopeJSON: roleWriteScopeJSON,
	})
}

func agentWorkLooksScopeRepairRole(role ProjectRoleRecord) bool {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		role.RoleID,
		role.Summary,
		role.UpdatedBy,
	}, "\n")))
	if text == "" {
		return false
	}
	if strings.Contains(text, "stale role") ||
		strings.Contains(text, "stale repair role") ||
		strings.Contains(text, "do not override") ||
		strings.Contains(text, "not override") ||
		strings.Contains(text, "should not override") ||
		strings.Contains(text, "must not override") {
		return false
	}
	if agentWorkLooksBoundaryTransitionRole(role) {
		return true
	}
	hasRepairIntent := strings.Contains(text, "claim repair") ||
		strings.Contains(text, "blocked admission") ||
		strings.Contains(text, "scope repair") ||
		strings.Contains(text, "boundary expansion") ||
		strings.Contains(text, "expand_boundary") ||
		strings.Contains(text, "expanded boundary") ||
		strings.Contains(text, "boundary transition") ||
		strings.Contains(text, "side-effect resolution") ||
		strings.Contains(text, "side effect resolution") ||
		((strings.Contains(text, "repair") || strings.Contains(text, "repaired")) &&
			(strings.Contains(text, "narrow") || strings.Contains(text, "narrowing")))
	if !hasRepairIntent {
		return false
	}
	return strings.Contains(text, "scope") ||
		strings.Contains(text, "write_scope") ||
		strings.Contains(text, "write scope") ||
		strings.Contains(text, "ownership") ||
		strings.Contains(text, "owner")
}

func agentWorkLooksBoundaryTransitionRole(role ProjectRoleRecord) bool {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		role.RoleID,
		role.Summary,
		role.UpdatedBy,
	}, "\n")))
	if text == "" {
		return false
	}
	if strings.Contains(text, "stale role") ||
		strings.Contains(text, "do not override") ||
		strings.Contains(text, "not override") ||
		strings.Contains(text, "should not override") ||
		strings.Contains(text, "must not override") {
		return false
	}
	return strings.Contains(text, "expand_boundary") ||
		strings.Contains(text, "boundary expansion") ||
		strings.Contains(text, "expanded boundary") ||
		strings.Contains(text, "boundary transition") ||
		strings.Contains(text, "side-effect resolution") ||
		strings.Contains(text, "side effect resolution") ||
		strings.Contains(text, "abpc side-effect") ||
		strings.Contains(text, "abpc side effect")
}

func agentWorkWriteScopeJSONIsBroad(writeScopeJSON string) bool {
	paths := writeScopePaths(writeScopeJSON)
	if len(paths) != 1 {
		return false
	}
	switch strings.Trim(strings.ToLower(strings.TrimSpace(paths[0])), "/") {
	case "*", "**":
		return true
	default:
		return false
	}
}

func agentWorkRevisionTaskScopeRepairShape(task WorkspaceTaskRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, "\n")))
	if text == "" {
		return false
	}
	hasRevisionIntent := agentWorkTaskHasTag(task, "revision") ||
		strings.Contains(text, "revision") ||
		strings.Contains(text, "revise") ||
		strings.Contains(text, "unblock integration candidate") ||
		strings.Contains(text, "blocked candidate") ||
		strings.Contains(text, "validation-followup") ||
		strings.Contains(text, "validation followup")
	if !hasRevisionIntent {
		return false
	}
	return strings.Contains(text, "patch queue") ||
		strings.Contains(text, "patch-queue") ||
		strings.Contains(text, "patchq-") ||
		strings.Contains(text, "patchitem-") ||
		strings.Contains(text, "queue_id") ||
		strings.Contains(text, "item_id") ||
		strings.Contains(text, "branch_id") ||
		strings.Contains(text, "blocked candidate")
}

func agentWorkWriteScopeLooksCandidateWide(paths []string) bool {
	for _, path := range paths {
		normalized := strings.Trim(strings.ToLower(strings.TrimSpace(path)), "/")
		switch normalized {
		case "*", "**", "src", "src/**", "app", "app/**", "web", "web/**", "client", "client/**", "tests", "tests/**", "test", "test/**":
			return true
		}
	}
	return false
}

func agentWorkSemanticTaskWriteScopeHints(task WorkspaceTaskRecord, paths []string) []string {
	if agentWorkRevisionTaskScopeRepairShape(task) {
		return nil
	}
	text := agentWorkTaskSemanticScopeText(task)
	if text == "" {
		return nil
	}
	if luaScope := agentWorkLuaAcceptanceWriteScopeHints(task); len(luaScope) > 0 && agentWorkLuaAcceptanceScopeShouldNarrow(paths, luaScope) {
		return luaScope
	}
	if !agentWorkTaskScopeNeedsSemanticNarrowing(paths) {
		return nil
	}
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	if agentWorkTaskLooksGoInterpreterScope(task, paths) {
		implementationText := agentWorkSemanticTextWithoutNegatedImplementationClauses(text)
		if implementationText == "" {
			return nil
		}
		if primary := agentWorkGoInterpreterPrimaryWriteScopeHints(task, paths); len(primary) > 0 {
			return primary
		}
		if agentWorkScopeTextContainsAny(implementationText, "lexer", "lexical", "tokenizer", "tokeniser", "token stream") {
			add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
		}
		if agentWorkScopeTextContainsAny(implementationText, "parser", "parse", "grammar") {
			add("internal/parser/**", "internal/ast/**")
		}
		evaluatorByName := agentWorkScopeTextContainsAny(implementationText, "evaluator", "evaluation", "evaluate") ||
			(agentWorkSemanticTextHasToken(implementationText, "eval") && !agentWorkScopeTextContainsAny(implementationText, "no eval", "no-eval", "not eval", "without eval"))
		evaluatorBySemantics := agentWorkScopeTextContainsAny(implementationText, "json path", "jsonpath", "path semantics", "query semantics") &&
			!agentWorkScopeTextContainsAny(implementationText, "parser", "parse", "grammar", "syntax", "ast")
		if evaluatorByName || evaluatorBySemantics {
			add("internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**")
		}
		if agentWorkTaskLooksGoBuiltinsImplementationScope(implementationText) {
			add("internal/stdlib/**", "internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/lambda/**")
		}
		if agentWorkScopeTextContainsAny(implementationText, "cli", "command line", "file mode", "repl", "read-eval-print") {
			add("cmd/**", "internal/cli/**", "internal/repl/**")
		}
		if agentWorkTaskLooksGoTestSuiteScope(implementationText) {
			add(agentWorkGoInterpreterTestScope(paths)...)
		}
		if agentWorkScopeTextContainsAny(implementationText, "error model", "diagnostic", "diagnostics") && !agentWorkScopeTextContainsAny(implementationText, "lexer", "lexical", "tokenizer", "tokeniser") {
			add("internal/errors/**", "internal/diagnostics/**")
		}
		if len(out) > 0 {
			narrowed := normalizeStringSlice(out)
			if filtered, ok := agentWorkFilterGoSemanticScopeByHints(paths, narrowed); ok {
				return filtered
			}
			return narrowed
		}
		// R20-F1: a Go-interpreter task that matched NO specific lane family keeps its seeded
		// scope untouched (nil = no semantic narrowing). Falling through into the JS-app
		// template sections below bound the R20 root `**` lane ("extend baseline") to a stale
		// frontend/Vite scope in a Go repo, which poisoned branch authority and cascaded into
		// side-effect/role-scope churn. A Go repo must never receive a JS scaffold scope.
		return nil
	}
	if agentWorkScopeTextContainsAny(text, "foundation", "toolchain", "app shell", "shared config", "baseline test harness") {
		add(".gitignore", "package*.json", "tsconfig*.json", "vite.config.*", "vitest.config.*", "playwright.config.*", "index.html", "public/**", "src/main.*", "src/App.*", "src/styles.*", "src/styles/**", "src/ui/**", "tests/setup.*", "tests/test-utils/**", "tests/fixtures/**")
		return normalizeStringSlice(out)
	}
	if agentWorkScopeTextContainsAny(text, "editor", "rich-text", "rich text", "markdown", "shortcut", "autosave", "quote", "quotes", "dash replacement", "blockquote", "divider") {
		add("src/editor/**", "src/lib/editor/**", "tests/editor/**")
	}
	if agentWorkScopeTextContainsAny(text, "settings", "preferences", "quote style", "auto dash", "auto-dash") {
		add("src/settings/**", "tests/settings/**")
	}
	if agentWorkSemanticTextHasToken(text, "auth", "authentication", "oauth") || agentWorkScopeTextContainsAny(text, "sign-in", "signin", "login", "google sign") {
		add("src/auth/**", "tests/auth/**")
	}
	if agentWorkScopeTextContainsAny(text, "profile", "avatar", "author profile") {
		add("src/profile/**", "tests/profile/**")
	}
	if agentWorkScopeTextContainsAny(text, "article management", "my articles", "article list", "article lifecycle", "draft", "published", "archive", "archiving", "delete article", "article search") {
		add("src/articles/**", "tests/articles/**")
	}
	if agentWorkScopeTextContainsAny(text, "public article", "public route", "read-only", "readonly", "/p/", "slug", "share url", "viewer") {
		add("src/public/**", "src/routes/**", "tests/public/**")
	}
	if agentWorkScopeTextContainsAny(text, "import/export", "import article", "export article", "export json", "import json", "serialization") {
		add("src/import-export/**", "src/lib/import-export/**", "tests/import-export/**")
	}
	if len(out) == 0 && agentWorkScopeTextContainsAny(text, "scaffold", "test harness") {
		add(".gitignore", "package*.json", "tsconfig*.json", "vite.config.*", "vitest.config.*", "playwright.config.*", "index.html", "public/**", "src/main.*", "src/App.*", "src/styles.*", "src/styles/**", "src/ui/**", "tests/setup.*", "tests/test-utils/**", "tests/fixtures/**")
	}
	return normalizeStringSlice(out)
}

func agentWorkSemanticTaskWriteScopeHintsWithoutExplicitHints(task WorkspaceTaskRecord) []string {
	if !agentWorkTaskLooksGoInterpreterScope(task, nil) {
		return nil
	}
	if paths := agentWorkLuaAcceptanceWriteScopeHints(task); len(paths) > 0 {
		return paths
	}
	paths := agentWorkGoInterpreterPrimaryWriteScopeHints(task, nil)
	text := agentWorkTaskSemanticScopeText(task)
	if agentWorkScopeTextContainsAny(text, "readme", "documentation", "docs") {
		paths = append(paths, "README.md")
	}
	if len(paths) > 0 {
		return normalizeStringSlice(paths)
	}
	return agentWorkSemanticTaskWriteScopeHints(task, []string{"internal", "cmd", "README.md", "tests", "**/*_test.go", "testdata/**"})
}

func agentWorkGoInterpreterPrimaryWriteScopeHints(task WorkspaceTaskRecord, paths []string) []string {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.ProjectLane,
	}, "\n")))
	if text == "" {
		return nil
	}
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	hasLexer := agentWorkScopeTextContainsAny(text, "lexer", "lexical", "tokenizer", "tokeniser", "token stream")
	hasParser := agentWorkScopeTextContainsAny(text, "parser", "parse", "grammar", "ast")
	hasEvaluator := agentWorkScopeTextContainsAny(text, "evaluator", "evaluation", "evaluate", " eval ", " eval-", "-eval", "json path", "jsonpath", "query semantics")
	if agentWorkScopeTextContainsAny(text, "no-eval", "no eval", "not eval", "without eval", "no evaluator", "not evaluator", "without evaluator") {
		hasEvaluator = false
	}
	hasBuiltins := agentWorkScopeTextContainsAny(text, "built-in", "builtin", "builtins", "stdlib", "standard library", "map/filter", "map filter", "lambda semantics", "lambda runtime", "lambda execution", "function library")
	hasCLI := agentWorkScopeTextContainsAny(text, "cli", "command line", "file mode", "repl", "read-eval-print")
	hasTestSuite := agentWorkTaskLooksGoTestSuiteScope(text)
	if hasLexer {
		add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
	}
	if hasParser {
		add("internal/parser/**", "internal/ast/**")
	}
	if hasEvaluator {
		add("internal/eval/**", "internal/jsonctx/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**")
	}
	if hasBuiltins && !hasParser {
		add("internal/stdlib/**", "internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/lambda/**")
	}
	if hasCLI {
		if agentWorkScopeTextContainsAny(text, "lua", "conformance", "oracle", "harness") {
			add(agentWorkGoInterpreterHarnessCommandScope(paths)...)
		} else {
			add(agentWorkGoInterpreterCommandScope(paths)...)
		}
		if agentWorkScopeTextContainsAny(text, "readme", "reviewer-facing") {
			add("README.md")
		}
	}
	if hasTestSuite {
		add(agentWorkGoInterpreterTestScope(paths)...)
	}
	return normalizeStringSlice(out)
}

func agentWorkLuaAcceptanceWriteScopeHints(task WorkspaceTaskRecord) []string {
	text := agentWorkTaskSemanticScopeText(task)
	if !strings.Contains(text, "ac-lua-") {
		return nil
	}
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	has := func(id string) bool {
		return strings.Contains(text, strings.ToLower(id))
	}
	if has("AC-LUA-LEX-01") {
		add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
	}
	if has("AC-LUA-PARSE-01") {
		add("internal/parser/**", "internal/ast/**")
	}
	if has("AC-LUA-SEM-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**")
	}
	if has("AC-LUA-FUNC-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/functions/**")
	}
	if has("AC-LUA-TABLE-01") || has("AC-LUA-META-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/table/**", "internal/metatable/**")
	}
	if has("AC-LUA-STDLIB-01") {
		add("internal/stdlib/**", "internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/runtime/**", "internal/value/**")
	}
	if has("AC-LUA-ERR-01") {
		add("internal/errors/**", "internal/diagnostics/**", "internal/runner/**")
	}
	if has("AC-LUA-CLI-01") {
		add("cmd/glua/**", "internal/cli/**", "internal/repl/**", "internal/runner/**", "scripts/**", "testdata/smoke/**", "tools/oracle/**", "README.md")
	}
	return normalizeStringSlice(out)
}

func agentWorkLuaAcceptanceScopeShouldNarrow(paths, allowed []string) bool {
	paths = normalizeStringSlice(paths)
	allowed = normalizeStringSlice(allowed)
	if len(paths) == 0 || len(allowed) == 0 {
		return len(allowed) > 0
	}
	if agentWorkTaskScopeNeedsSemanticNarrowing(paths) {
		return true
	}
	for _, path := range paths {
		if !agentWorkScopePathCoveredByAnyHint(path, allowed) {
			return true
		}
	}
	return false
}

func agentWorkGoInterpreterCommandScope(paths []string) []string {
	for _, path := range normalizeStringSlice(paths) {
		normalized := normalizeWriteScopePath(path)
		if normalized == "cmd" || strings.HasPrefix(normalized, "cmd/") {
			return normalizeStringSlice([]string{path, "internal/cli/**", "internal/repl/**"})
		}
	}
	return []string{"cmd/**", "internal/cli/**", "internal/repl/**"}
}

func agentWorkGoInterpreterHarnessCommandScope(paths []string) []string {
	var out []string
	for _, path := range normalizeStringSlice(paths) {
		normalized := normalizeWriteScopePath(path)
		if normalized == "readme.md" ||
			normalized == "cmd" ||
			strings.HasPrefix(normalized, "cmd/") ||
			normalized == "internal/cli" ||
			strings.HasPrefix(normalized, "internal/cli/") ||
			normalized == "internal/repl" ||
			strings.HasPrefix(normalized, "internal/repl/") ||
			normalized == "internal/runner" ||
			strings.HasPrefix(normalized, "internal/runner/") ||
			normalized == "scripts" ||
			strings.HasPrefix(normalized, "scripts/") ||
			normalized == "testdata" ||
			strings.HasPrefix(normalized, "testdata/") ||
			normalized == "tools/oracle" ||
			strings.HasPrefix(normalized, "tools/oracle/") {
			out = append(out, path)
		}
	}
	if len(out) > 0 {
		return normalizeStringSlice(out)
	}
	return normalizeStringSlice(append(agentWorkGoInterpreterCommandScope(paths), "scripts/**", "testdata/**", "tools/oracle/**"))
}

func agentWorkTaskLooksGoTestSuiteScope(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return agentWorkScopeTextContainsAny(text,
		"test suite",
		"test-suite",
		"test harness",
		"golden test",
		"golden tests",
		"table-driven",
		"testdata",
		"fixture",
		"fixtures",
	) || agentWorkSemanticTextHasToken(text, "tests", "testing")
}

func agentWorkGoInterpreterTestScope(paths []string) []string {
	out := make([]string, 0, 4)
	for _, path := range normalizeStringSlice(paths) {
		normalized := normalizeWriteScopePath(path)
		if strings.Contains(normalized, "_test.go") ||
			strings.Contains(normalized, "test.go") ||
			normalized == "testdata" ||
			strings.HasPrefix(normalized, "testdata/") ||
			normalized == "tests" ||
			strings.HasPrefix(normalized, "tests/") {
			out = append(out, path)
		}
	}
	if len(out) == 0 {
		out = append(out, "**/*_test.go", "testdata/**")
	}
	return normalizeStringSlice(out)
}

func agentWorkTaskLooksGoInterpreterScope(task WorkspaceTaskRecord, paths []string) bool {
	if agentWorkScopePathsContainGoLayout(paths) {
		return true
	}
	text := agentWorkTaskSemanticScopeText(task)
	if text == "" {
		return false
	}
	if agentWorkSemanticTextHasToken(text, "rq", "golang", "lua") ||
		agentWorkScopeTextContainsAny(text, ".go", "go module", "go.mod", "go sum", "go.sum", "go work", "go.work", "interpreter", "json path", "jsonpath", "lexer", "lexical", "tokenizer", "tokeniser", "token stream", "evaluator", "evaluation", "evaluate", "stdlib", "standard library", "builtin", "builtins", "read-eval-print") {
		return true
	}
	return agentWorkSemanticTextHasToken(text, "go") && !agentWorkScopeTextContainsAny(text, "react", "vite", "frontend", "browser", "web app", "typescript", "javascript", "src/")
}

func agentWorkScopePathsContainGoLayout(paths []string) bool {
	for _, path := range paths {
		normalized := normalizeWriteScopePath(path)
		switch normalized {
		case "cmd", "internal", "pkg", "go.mod", "go.sum", "go.work":
			return true
		}
		if strings.HasPrefix(normalized, "cmd/") ||
			strings.HasPrefix(normalized, "internal/") ||
			strings.HasPrefix(normalized, "pkg/") ||
			normalized == "scripts" ||
			strings.HasPrefix(normalized, "scripts/") ||
			normalized == "testdata" ||
			strings.HasPrefix(normalized, "testdata/") ||
			normalized == "tools/oracle" ||
			strings.HasPrefix(normalized, "tools/oracle/") {
			return true
		}
	}
	return false
}

func agentWorkFilterGoSemanticScopeByHints(hints, semantic []string) ([]string, bool) {
	allowedFamilies := agentWorkGoSemanticScopeFamiliesAllowedByHints(hints)
	if len(allowedFamilies) == 0 {
		return semantic, false
	}
	if allowedFamilies["all"] {
		return semantic, false
	}
	var out []string
	seen := map[string]struct{}{}
	for _, path := range semantic {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		allowed := agentWorkScopePathCoveredByAnyHint(path, hints)
		if !allowed {
			family := agentWorkGoSemanticScopePathFamily(path)
			allowed = family != "" && allowedFamilies[family]
		}
		if !allowed {
			continue
		}
		key := strings.ToLower(path)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, path)
	}
	return normalizeStringSlice(out), true
}

func agentWorkScopePathCoveredByAnyHint(path string, hints []string) bool {
	for _, hint := range hints {
		if writeScopePathCoveredBy(path, hint) {
			return true
		}
	}
	return false
}

func agentWorkGoSemanticScopeFamiliesAllowedByHints(hints []string) map[string]bool {
	allowed := map[string]bool{}
	for _, hint := range hints {
		normalized := normalizeWriteScopePath(hint)
		switch normalized {
		case "*", "**", "internal", "pkg":
			allowed["all"] = true
		case "internal/lexer", "internal/token", "internal/tokens":
			allowed["lexer"] = true
		case "internal/parser", "internal/ast":
			allowed["parser"] = true
		case "internal/eval", "internal/jsonctx", "internal/evaluator", "internal/runtime", "internal/value", "internal/path", "internal/jsonpath":
			allowed["evaluator"] = true
		case "internal/stdlib", "internal/builtins", "internal/builtin", "internal/functions", "internal/lambda":
			allowed["builtins"] = true
		case "cmd", "internal/cli", "internal/repl":
			allowed["cli"] = true
		case "internal/errors", "internal/diagnostics":
			allowed["diagnostics"] = true
		default:
			if strings.HasPrefix(normalized, "internal/") || strings.HasPrefix(normalized, "cmd/") || strings.HasSuffix(normalized, ".go") {
				allowed["specific"] = true
			}
		}
	}
	return allowed
}

func agentWorkGoSemanticScopePathFamily(path string) string {
	switch normalizeWriteScopePath(path) {
	case "internal/lexer", "internal/token", "internal/tokens":
		return "lexer"
	case "internal/parser", "internal/ast":
		return "parser"
	case "internal/eval", "internal/jsonctx", "internal/evaluator", "internal/runtime", "internal/value", "internal/path", "internal/jsonpath":
		return "evaluator"
	case "internal/stdlib", "internal/builtins", "internal/builtin", "internal/functions", "internal/lambda":
		return "builtins"
	case "cmd", "internal/cli", "internal/repl":
		return "cli"
	case "internal/errors", "internal/diagnostics":
		return "diagnostics"
	default:
		return ""
	}
}

func agentWorkTaskLooksGoBuiltinsImplementationScope(text string) bool {
	text = agentWorkSemanticTextWithoutNegatedImplementationClauses(text)
	if text == "" {
		return false
	}
	return agentWorkScopeTextContainsAny(text,
		"built-in", "builtin", "builtins", "stdlib", "standard library",
		"function library",
		"lambda helper", "lambda helpers",
		"lambda runtime", "lambda execution", "lambda evaluation", "lambda semantics",
		"implement lambda", "implement lambdas",
		"map/filter builtin", "map/filter builtins",
		"map filter builtin", "map filter builtins",
	)
}

func agentWorkSemanticTextWithoutNegatedImplementationClauses(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	splitter := func(r rune) bool {
		switch r {
		case '.', '\n', ';':
			return true
		default:
			return false
		}
	}
	parts := strings.FieldsFunc(text, splitter)
	var kept []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lower := strings.ToLower(part)
		if agentWorkScopeTextContainsAny(lower,
			"do not implement",
			"don't implement",
			"without implementing",
			"without implementation",
			"not implement",
			"not implementing",
			"out of scope",
		) {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "\n")
}

func agentWorkTaskScopeNeedsSemanticNarrowing(paths []string) bool {
	for _, path := range paths {
		switch normalizeWriteScopePath(path) {
		case "*", "**", "src", "app", "web", "client", "tests", "test", "cmd", "internal", "pkg", "lib", "go.mod", "go.sum", "go.work", "readme", "readme.md", "**/*test.go", "**/*_test.go", "*_test.go":
			return true
		}
	}
	return false
}

func agentWorkTaskSemanticScopeText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
		task.TaskRequirementsJSON,
	}, "\n")))
}

func agentWorkScopeTextContainsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func agentWorkSemanticTextHasToken(text string, tokens ...string) bool {
	if text == "" {
		return false
	}
	normalized := strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ").Replace(strings.ToLower(text))
	fields := strings.Fields(normalized)
	for _, field := range fields {
		for _, token := range tokens {
			if field == strings.ToLower(strings.TrimSpace(token)) {
				return true
			}
		}
	}
	return false
}

func agentWorkTaskRequirementWriteScopeHints(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	switch value := payload["write_scope_hints"].(type) {
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return normalizeStringSlice(out)
	case []string:
		return normalizeStringSlice(value)
	case string:
		return normalizeStringSlice([]string{value})
	default:
		return nil
	}
}

func unresolvedAgentWorkDependencyIDs(blockers map[string][]string, taskID string) []string {
	if len(blockers) == 0 {
		return nil
	}
	return uniqueTrimmedAgentWork(blockers[strings.TrimSpace(taskID)])
}

func taskDependencyBlockedPacket(task WorkspaceTaskRecord, blockers []string) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	blockers = uniqueTrimmedAgentWork(blockers)
	summary := fmt.Sprintf("task %s is blocked by unresolved dependency task(s): %s", taskID, strings.Join(blockers, ", "))
	return &AgentWorkPacket{
		WorkType:            "task_dependency_blocked",
		ProjectID:           strings.TrimSpace(task.ProjectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_dependency_resolution",
		PreferredTransition: "complete_dependency_or_replan",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys:      []string{"task." + taskID},
			AnchorTaskIDs:         uniqueTrimmedAgentWork(append([]string{taskID}, blockers...)),
			AnchorConflictTaskIDs: blockers,
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "task_dependency",
			NeededFrom: "dependency_task",
			Summary:    summary,
		},
	}
}

func projectClaimAdmissionUnclaimablePacket(task WorkspaceTaskRecord, summary string) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	summary = firstNonEmpty(strings.TrimSpace(summary), "Task would require project claim bindings but is not attached to a project.")
	return &AgentWorkPacket{
		WorkType:            "project_claim_admission_unclaimable",
		ProjectID:           strings.TrimSpace(task.ProjectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: projectTaskRequiresImplementationGate(task),
		CoordinationState:   "claim_admission_blocked",
		PreferredTransition: "repair_task_project_binding_or_terminalize_stale_task",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_claim_admission_parity",
			NeededFrom: "task_project_fields_updated",
			Summary:    summary,
		},
	}
}

func taskSupersededPacket(task WorkspaceTaskRecord) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	return &AgentWorkPacket{
		WorkType:            "trigger_task_superseded",
		ProjectID:           strings.TrimSpace(task.ProjectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "task_superseded",
		PreferredTransition: "inspect_current_coordination_state",
		WhyNow:              fmt.Sprintf("targeted task %s is superseded by newer project or patch-queue evidence; do not execute the stale task", taskID),
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    []string{taskID},
		},
		Decision: &AgentWorkDecisionPacket{
			NeededFrom:   "workspace.tasks.list/project_coordination",
			DecisionType: "superseded_task_disposition",
		},
	}
}

func projectImplementationTargetedDelegationPacket(task WorkspaceTaskRecord, projectID, summary string) *AgentWorkPacket {
	summary = firstNonEmpty(strings.TrimSpace(summary), "project implementation task is reserved for targeted project-role delegation")
	return &AgentWorkPacket{
		WorkType:            "project_targeted_delegation_required",
		ProjectID:           strings.TrimSpace(projectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_targeted_delegation",
		PreferredTransition: "runtime_switch_task",
		WhyNow:              summary,
		Gate: &AgentWorkGatePacket{
			GateState:  "closed",
			GateType:   "project_role_targeted_delegation",
			NeededFrom: "strategic_lead",
			Summary:    summary,
		},
	}
}

func workspaceTaskPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func projectImplementationClaimScopeBusyPacket(task WorkspaceTaskRecord, projectID, summary, conflictTaskID, conflictBranchID, conflictOwnerAgentID string) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	summary = firstNonEmpty(strings.TrimSpace(summary), "project implementation write scope is currently owned by another active claim")
	conflictOwnerAgentID = strings.TrimSpace(conflictOwnerAgentID)
	preferredTransition := "request_strategic_repair"
	neededFrom := "strategic_lead"
	if conflictOwnerAgentID != "" {
		preferredTransition = "delegate_to_branch_owner"
		neededFrom = conflictOwnerAgentID
	}
	packet := &AgentWorkPacket{
		WorkType:            "project_claim_scope_busy",
		ProjectID:           strings.TrimSpace(projectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "project_claim_scope_busy",
		PreferredTransition: preferredTransition,
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys:      []string{"task." + taskID},
			AnchorTaskIDs:         uniqueTrimmedAgentWork([]string{taskID}),
			AnchorConflictTaskIDs: uniqueTrimmedAgentWork([]string{conflictTaskID}),
			AnchorBranchIDs:       uniqueTrimmedAgentWork([]string{conflictBranchID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_claim_scope_busy",
			NeededFrom: neededFrom,
			Summary:    summary,
		},
	}
	if conflictOwnerAgentID != "" {
		packet.HandoffToAgentID = conflictOwnerAgentID
		packet.Handoff = &AgentWorkHandoffPacket{
			HandoffState: "branch_owner_required",
			ToAgentID:    conflictOwnerAgentID,
			Summary:      summary,
		}
	}
	return packet
}

func projectClaimRepairLeadRequiredPacket(task WorkspaceTaskRecord) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	summary := "project claim repair coordination task is reserved for the active strategic lead"
	return &AgentWorkPacket{
		WorkType:            "project_claim_repair_lead_required",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_strategic_lead_repair",
		PreferredTransition: "delegate_to_strategic_lead",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_claim_repair_lead_required",
			NeededFrom: "strategic_lead",
			Summary:    summary,
		},
	}
}

func projectStrategicLeadCoordinationRequiredPacket(task WorkspaceTaskRecord) *AgentWorkPacket {
	if !projectRoleScopeTask(task) {
		return projectClaimRepairLeadRequiredPacket(task)
	}
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	summary := "project role/scope coordination task requires the active strategic lead to execute project_role_assign; chat/status approval is not a transition"
	return &AgentWorkPacket{
		WorkType:            "project_role_scope_authority_transition_required",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_strategic_lead_boundary_transition",
		PreferredTransition: "project_role_assign",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_role_scope_authority_transition",
			NeededFrom: "strategic_lead",
			Summary:    summary,
		},
	}
}

func projectOwnerBoundAgentRequiredPacket(task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement) *AgentWorkPacket {
	ownerID := strings.TrimSpace(req.RequiredAgentID)
	summary := fmt.Sprintf("owner-bound %s task requires branch owner %s", firstNonEmpty(strings.TrimSpace(req.Kind), "branch mutation"), firstNonEmpty(ownerID, "unknown"))
	packet := projectOwnerBoundPacket(task, req, "project_owner_bound_agent_required", "waiting_for_branch_owner", "delegate_to_branch_owner", summary)
	packet.HandoffToAgentID = ownerID
	packet.Handoff = &AgentWorkHandoffPacket{
		HandoffState: "owner_bound_required",
		ToAgentID:    ownerID,
		Summary:      summary,
	}
	packet.Gate = &AgentWorkGatePacket{
		GateState:  ProjectGateStateBlocked,
		GateType:   "project_owner_bound_agent_required",
		NeededFrom: ownerID,
		Summary:    summary,
	}
	return packet
}

func projectOwnerBoundRepairRequiredPacket(task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement) *AgentWorkPacket {
	summary := firstNonEmpty(strings.TrimSpace(req.Reason), "owner-bound task cannot resolve a concrete branch owner")
	packet := projectOwnerBoundPacket(task, req, "project_owner_bound_repair_required", "owner_bound_repair_required", "request_strategic_repair", summary)
	packet.Gate = &AgentWorkGatePacket{
		GateState:  ProjectGateStateBlocked,
		GateType:   "project_owner_bound_repair_required",
		NeededFrom: "strategic_lead",
		Summary:    summary,
	}
	return packet
}

func projectOwnerBoundWrongClaimPacket(task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement, claimAgentID string) *AgentWorkPacket {
	ownerID := strings.TrimSpace(req.RequiredAgentID)
	summary := fmt.Sprintf("owner-bound task is claimed by %s but branch owner is %s", strings.TrimSpace(claimAgentID), firstNonEmpty(ownerID, "unknown"))
	packet := projectOwnerBoundPacket(task, req, "project_owner_bound_wrong_claim", "owner_bound_wrong_claim", "release_wrong_claim_or_delegate_to_branch_owner", summary)
	packet.HandoffToAgentID = ownerID
	packet.Handoff = &AgentWorkHandoffPacket{
		HandoffState: "wrong_claim_repair",
		ToAgentID:    ownerID,
		Summary:      summary,
	}
	packet.Gate = &AgentWorkGatePacket{
		GateState:  ProjectGateStateBlocked,
		GateType:   "project_owner_bound_wrong_claim",
		NeededFrom: firstNonEmpty(ownerID, "strategic_lead"),
		Summary:    summary,
	}
	packet.ContextHints.AnchorConflictTaskIDs = uniqueTrimmedAgentWork([]string{strings.TrimSpace(task.TaskID)})
	return packet
}

func projectOwnerBoundPacket(task WorkspaceTaskRecord, req agentWorkOwnerBoundRequirement, workType, state, transition, summary string) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	branchID := strings.TrimSpace(req.BranchID)
	return &AgentWorkPacket{
		WorkType:            strings.TrimSpace(workType),
		ProjectID:           strings.TrimSpace(task.ProjectID),
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   strings.TrimSpace(state),
		PreferredTransition: strings.TrimSpace(transition),
		WhyNow:              strings.TrimSpace(summary),
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
			AnchorBranchIDs:  uniqueTrimmedAgentWork([]string{branchID}),
		},
		OwnerBound: &AgentWorkOwnerBoundPacket{
			Kind:            strings.TrimSpace(req.Kind),
			RequiredAgentID: strings.TrimSpace(req.RequiredAgentID),
			BranchID:        branchID,
			BranchName:      strings.TrimSpace(req.BranchName),
			QueueID:         strings.TrimSpace(req.QueueID),
			ItemID:          strings.TrimSpace(req.ItemID),
			RepairNeeded:    req.RepairNeeded,
			Reason:          strings.TrimSpace(req.Reason),
		},
	}
}

func projectPatchQueueReviewRoleRequiredPacket(task WorkspaceTaskRecord) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	summary := "patch queue review task requires the active strategic lead, reviewer, integrator, or registered review/integration agent role"
	return &AgentWorkPacket{
		WorkType:            "project_patch_queue_review_role_required",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_patch_queue_reviewer",
		PreferredTransition: "select_review_or_integration_actor",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_patch_queue_review_role_required",
			NeededFrom: "reviewer_or_integrator",
			Summary:    summary,
		},
	}
}

func projectRoleLaneRequiredPacket(task WorkspaceTaskRecord) *AgentWorkPacket {
	taskID := strings.TrimSpace(task.TaskID)
	projectID := strings.TrimSpace(task.ProjectID)
	lane := strings.TrimSpace(task.ProjectLane)
	requiredRoles := projectClaimRequiredRoleTypesForLane(lane)
	roleSummary := projectClaimRequiredRoleSummary(requiredRoles)
	summary := fmt.Sprintf("project %s lane task requires an active %s role for this agent", lane, roleSummary)
	return &AgentWorkPacket{
		WorkType:            "project_role_lane_required",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         lane,
		RequiresProjectGate: task.RequiresProjectGate,
		CoordinationState:   "waiting_for_role_matched_agent",
		PreferredTransition: "select_role_matched_agent_or_replan",
		WhyNow:              summary,
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"task." + taskID},
			AnchorTaskIDs:    uniqueTrimmedAgentWork([]string{taskID}),
		},
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_role_lane_required",
			NeededFrom: roleSummary,
			Summary:    summary,
		},
	}
}

func projectImplementationGateClosedPacket(task WorkspaceTaskRecord, projectID, gateState, neededFrom, summary string) *AgentWorkPacket {
	packet := &AgentWorkPacket{
		WorkType:            "project_gate_closed",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		RequiresProjectGate: projectTaskRequiresImplementationGate(task),
		Gate: &AgentWorkGatePacket{
			GateState:  gateState,
			GateType:   "project_implementation_gate",
			NeededFrom: neededFrom,
			Summary:    summary,
		},
	}
	return packet
}

func projectValidationArtifactMissingPacket(task WorkspaceTaskRecord, projectID string) *AgentWorkPacket {
	return &AgentWorkPacket{
		WorkType:            "project_validation_artifact_missing",
		ProjectID:           projectID,
		TaskKind:            strings.TrimSpace(task.TaskKind),
		ProjectLane:         strings.TrimSpace(task.ProjectLane),
		CoordinationState:   "waiting_for_reviewable_artifact",
		PreferredTransition: "wait_or_claim_implementation",
		WhyNow:              "validation/browser work requires a review-ready branch, patch queue candidate, or equivalent runnable artifact before it can produce truthful acceptance evidence",
		Gate: &AgentWorkGatePacket{
			GateState:  ProjectGateStateBlocked,
			GateType:   "project_validation_artifact",
			NeededFrom: "project.branch.review_ready",
			Summary:    "No review-ready branch, patch queue candidate, or durable runnable artifact is available for validation yet.",
		},
		ContextHints: AgentWorkContextHints{
			AnchorTaskIDs: []string{strings.TrimSpace(task.TaskID)},
		},
	}
}

func projectImplementationGateClosedSummary(status ProjectGateStatusRecord) string {
	for _, gate := range status.Gates {
		if !gate.Required || projectGateSatisfied(gate.State) {
			continue
		}
		if gate.Summary != "" {
			return gate.GateKey + ": " + gate.Summary
		}
		return gate.GateKey + " is " + strings.ToLower(gate.State)
	}
	return "Project implementation gates are not satisfied"
}

func enrichAgentWorkHydrationDocKeys(taskID, projectID string, requested []string) []string {
	keys := append([]string(nil), requested...)
	taskID = strings.TrimSpace(taskID)
	if taskID != "" {
		keys = append(keys, hydrationTaskDocKeys(taskID)...)
	}
	if projectID = strings.TrimSpace(projectID); projectID != "" {
		keys = append(keys, hydrationProjectPlanningDocKeys(projectID)...)
	}
	for _, key := range []string{"current_context", "decisions", "open_questions", "handoff", "tooling", "autonomy_policy"} {
		keys = append(keys, key)
	}
	return uniqueTrimmedAgentWork(keys)
}

func unblockStateForAgentWork(result *AgentWorkNextResult) string {
	if result == nil || result.Session == nil {
		return ""
	}
	if trigger := strings.TrimSpace(result.Trigger); trigger != "" && shouldWakeAgentWorkSession(trigger, result.Session.Status) {
		return "wake_selected"
	}
	return "blocked"
}

func handoffStateForAgentWork(result *AgentWorkNextResult) string {
	if result == nil || result.Session == nil {
		return ""
	}
	if trigger := strings.TrimSpace(result.Trigger); trigger != "" && shouldWakeAgentWorkSession(trigger, result.Session.Status) {
		return "wake_selected"
	}
	return "pending"
}

func blockerKindsForAgentWork(items []model.AgentUpdateBlockedRef) []string {
	if len(items) == 0 {
		return nil
	}
	kinds := make([]string, 0, len(items))
	for _, item := range items {
		if kind := strings.TrimSpace(item.Kind); kind != "" {
			kinds = append(kinds, kind)
		}
	}
	return uniqueTrimmedAgentWork(kinds)
}

func coordinationStateForAgentWork(result *AgentWorkNextResult) string {
	if result == nil {
		return ""
	}
	if result.Task != nil && projectRoleScopeTask(*result.Task) {
		return "waiting_for_strategic_lead_boundary_transition"
	}
	if result.Session != nil {
		switch strings.ToUpper(strings.TrimSpace(result.Session.Status)) {
		case model.SessionStatusBlocked:
			return "blocked"
		case model.SessionStatusWaitingDecision:
			return "waiting_decision"
		case model.SessionStatusHandoffPending:
			return "handoff_pending"
		case model.SessionStatusEnded:
			return "ended"
		default:
			return "active"
		}
	}
	switch strings.TrimSpace(result.Reason) {
	case "project_role_scope_authority_transition":
		return "waiting_for_strategic_lead_boundary_transition"
	case "project_claim_repair_authority_transition":
		return "waiting_for_strategic_lead_repair"
	case "resume_claim":
		return "claimed_without_session"
	case "next_pending":
		return "ready"
	default:
		return "ready"
	}
}

func preferredTransitionForAgentWork(result *AgentWorkNextResult) string {
	if result == nil {
		return ""
	}
	if result.Task != nil && projectRoleScopeTask(*result.Task) {
		return "project_role_assign"
	}
	if result.Task != nil && projectClaimRepairTask(*result.Task) {
		return "project_claim_repair_receipt"
	}
	if recovery, ok := agentWorkABPCRecoveryActionFromTaskPointer(result.Task); ok {
		return recovery.PreferredTransition
	}
	if result.Session == nil {
		if strings.TrimSpace(result.Reason) == "project_role_scope_authority_transition" {
			return "project_role_assign"
		}
		return "start_new"
	}
	switch strings.ToUpper(strings.TrimSpace(result.Session.Status)) {
	case model.SessionStatusWaitingDecision:
		return "await_decision"
	case model.SessionStatusBlocked:
		return "await_unblock"
	case model.SessionStatusHandoffPending:
		return "handoff"
	default:
		switch strings.TrimSpace(result.SessionAction) {
		case "start_new":
			return "start_new"
		default:
			return "continue"
		}
	}
}

func whyNowForAgentWork(result *AgentWorkNextResult) string {
	if result == nil {
		return ""
	}
	if result.Task != nil && projectRoleScopeTask(*result.Task) {
		return "project role/scope authority-transition task requires project_role_assign; chat/status approval is not a transition"
	}
	if result.Task != nil && projectClaimRepairTask(*result.Task) {
		return "project claim repair task is an authority-bearing strategic-lead repair lane; it must be durably claimed and executed before the blocked project lane can continue"
	}
	if recovery, ok := agentWorkABPCRecoveryActionFromTaskPointer(result.Task); ok {
		return recovery.Summary
	}
	if trigger := strings.TrimSpace(result.Trigger); trigger != "" {
		return trigger
	}
	switch strings.TrimSpace(result.Reason) {
	case "resume_session":
		return "resume_session"
	case "resume_claim":
		return "claimed_work"
	case "next_pending":
		return "scheduler"
	default:
		return strings.TrimSpace(result.Reason)
	}
}

func contextHintsForAgentWork(result *AgentWorkNextResult) AgentWorkContextHints {
	if result == nil {
		return AgentWorkContextHints{}
	}
	docKeys := make([]string, 0, 8)
	artifactRefs := make([]string, 0, 8)
	taskIDs := make([]string, 0, 8)
	sessionIDs := make([]string, 0, 4)

	if result.Task != nil {
		taskIDs = append(taskIDs, strings.TrimSpace(result.Task.TaskID))
	}
	if result.Session != nil {
		sessionIDs = append(sessionIDs, strings.TrimSpace(result.Session.SessionID))
		if taskID := strings.TrimSpace(result.Session.TaskID); taskID != "" {
			taskIDs = append(taskIDs, taskID)
		}
		docKeys = append(docKeys, result.Session.RelatedDocKeys...)
		for _, ref := range result.Session.RelatedArtifactRefs {
			if trimmed := strings.TrimSpace(ref.Ref); trimmed != "" {
				artifactRefs = append(artifactRefs, trimmed)
			}
		}
	}
	if result.Hydration != nil {
		for _, doc := range result.Hydration.Docs {
			if key := strings.TrimSpace(doc.DocKey); key != "" {
				docKeys = append(docKeys, key)
			}
		}
		for _, artifact := range result.Hydration.Artifacts {
			if ref := strings.TrimSpace(artifact.ArtifactRef); ref != "" {
				artifactRefs = append(artifactRefs, ref)
			}
		}
		for _, task := range result.Hydration.RelatedTasks {
			if taskID := strings.TrimSpace(task.TaskID); taskID != "" {
				taskIDs = append(taskIDs, taskID)
			}
		}
	}
	return AgentWorkContextHints{
		SuggestedDocKeys:    uniqueTrimmedAgentWork(docKeys),
		RelatedArtifactRefs: uniqueTrimmedAgentWork(artifactRefs),
		AnchorTaskIDs:       uniqueTrimmedAgentWork(taskIDs),
		AnchorSessionIDs:    uniqueTrimmedAgentWork(sessionIDs),
	}
}

func (s *Store) buildAgentWorkAdvisory(ctx context.Context, result *AgentWorkNextResult, filter AgentWorkNextFilter) (AgentWorkAdvisory, bool) {
	if result == nil || result.Task == nil {
		return AgentWorkAdvisory{}, false
	}
	frontierLimit := filter.FrontierLimit
	if frontierLimit <= 0 {
		frontierLimit = 3
	}

	taskID := strings.TrimSpace(result.Task.TaskID)
	sessionID := ""
	if result.Session != nil {
		sessionID = strings.TrimSpace(result.Session.SessionID)
	}

	frontier, err := s.ListTensionFrontier(ctx, TensionFilter{
		WorkspaceID: strings.TrimSpace(result.WorkspaceID),
		TaskID:      taskID,
		Limit:       frontierLimit,
	})
	if err != nil {
		frontier = nil
	}

	clusterID := ""
	if len(frontier) > 0 {
		clusterID = strings.TrimSpace(frontier[0].ProtoClusterID)
	}
	if clusterID == "" {
		report, err := s.BuildControlReport(ctx, ControlReportFilter{
			WorkspaceID: strings.TrimSpace(result.WorkspaceID),
			Limit:       8,
		})
		if err == nil {
			clusterID = selectAgentWorkProtoCluster(report, taskID, sessionID, result.Hydration)
		}
	}
	if clusterID == "" {
		return AgentWorkAdvisory{}, false
	}

	advisory := AgentWorkAdvisory{
		ProtoClusterID: clusterID,
		Frontier:       filterAgentWorkFrontier(frontier, clusterID),
	}

	if detail, err := s.BuildControlClusterDetail(ctx, result.WorkspaceID, clusterID); err == nil {
		advisory.Control = &AgentWorkControlAdvisory{
			AttentionBand: strings.TrimSpace(detail.Cluster.Signals.AttentionBand),
			PressureScore: detail.Cluster.Signals.PressureScore,
			Summary:       strings.TrimSpace(detail.Cluster.Summary),
			BasisStale:    detail.Cluster.BasisStale,
		}
		if len(advisory.Frontier) == 0 {
			for _, tension := range detail.Tensions {
				advisory.Frontier = append(advisory.Frontier, tensionFrontierItemFromRecord(tension))
				if len(advisory.Frontier) >= frontierLimit {
					break
				}
			}
		}
	}
	if detail, err := s.BuildCorridorClusterDetail(ctx, result.WorkspaceID, clusterID); err == nil {
		advisory.Corridor = &AgentWorkCorridorAdvisory{
			CorridorReadiness:   strings.TrimSpace(detail.Cluster.CorridorReadiness),
			TaskClassHint:       strings.TrimSpace(detail.Cluster.TaskClassHint),
			CorridorCatalogHint: strings.TrimSpace(detail.Cluster.CorridorCatalogHint),
			Summary:             strings.TrimSpace(detail.Cluster.Summary),
			BasisStale:          detail.Cluster.BasisStale,
		}
	}
	return advisory, true
}

func (s *Store) buildAgentWorkTaskFrontier(ctx context.Context, workspaceID, agentID, generatedAt string, profile AgentProfileRecord, tasks []WorkspaceTaskRecord, sessions []AgentSessionStateRecord, taskDependencyBlocks map[string][]string, agentSessionTasks, pausedTasks, busyTasks map[string]struct{}, filter AgentWorkNextFilter, trustFirst bool, productLanePressureProjects map[string]struct{}) (*AgentWorkTaskFrontier, error) {
	limit := filter.FrontierLimit
	if limit <= 0 {
		limit = 5
	}
	if limit > 20 {
		limit = 20
	}

	candidates := make([]AgentWorkTaskFrontierCandidate, 0, limit)
	taskProjectByID := agentWorkTaskProjectByID(tasks)
	for _, task := range tasks {
		if isTerminalTaskStatus(task.Status) {
			continue
		}
		if _, hasSession := agentSessionTasks[strings.TrimSpace(task.TaskID)]; hasSession {
			continue
		}
		if _, paused := pausedTasks[strings.TrimSpace(task.TaskID)]; paused {
			continue
		}
		claimAction := claimActionForAgentWork(task, agentID)
		if !isResumableClaimForAgent(task, agentID) {
			if task.Status != model.TaskStatusPending || !claimAvailable(task.ClaimStatus) {
				continue
			}
		}
		if superseded, err := s.agentWorkTaskSuperseded(ctx, workspaceID, task); err != nil {
			return nil, err
		} else if superseded {
			continue
		}

		blockReason := ""
		blockSummary := ""
		advisoryReasons := make([]string, 0, 4)
		advisoryRoleTypes := projectClaimRequiredRoleTypesForLane(task.ProjectLane)

		if _, busy := busyTasks[strings.TrimSpace(task.TaskID)]; busy && !isResumableClaimForAgent(task, agentID) {
			blockReason = "task_busy_by_peer_session"
			blockSummary = "Another live agent session is already active for this task."
		}
		if blockReason == "" {
			if blockers := unresolvedAgentWorkDependencyIDs(taskDependencyBlocks, task.TaskID); len(blockers) > 0 {
				blockReason = "task_dependency_blocked"
				blockSummary = fmt.Sprintf("Blocked by unresolved dependency task(s): %s.", strings.Join(blockers, ", "))
			}
		}
		if blockReason == "" {
			if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
				return nil, err
			} else if blocked {
				blockReason = "project_gate_closed"
				if packet != nil && packet.Gate != nil {
					blockSummary = strings.TrimSpace(packet.Gate.Summary)
				}
				blockSummary = firstNonEmpty(blockSummary, "Project implementation gate is closed.")
			}
		}
		if blockReason == "" {
			if packet, blocked, err := s.agentWorkClaimAdmissionSelectionBlock(ctx, workspaceID, agentID, task, trustFirst); err != nil {
				return nil, err
			} else if blocked {
				blockReason = firstNonEmpty(strings.TrimSpace(packet.WorkType), "project_claim_admission_blocked")
				if packet != nil && packet.Gate != nil {
					blockSummary = strings.TrimSpace(packet.Gate.Summary)
				}
				if packet != nil {
					blockSummary = firstNonEmpty(blockSummary, strings.TrimSpace(packet.WhyNow), "Task would fail project claim admission.")
				} else {
					blockSummary = "Task would fail project claim admission."
				}
			}
		}
		if blockReason == "" {
			if ok, err := s.agentMaySelectProjectClaimRepairTask(ctx, workspaceID, agentID, task); err != nil {
				return nil, err
			} else if !ok {
				blockReason = "project_claim_repair_lead_required"
				blockSummary = "Project claim repair coordination task is reserved for the active strategic lead."
			}
		}
		if blockReason == "" {
			profileAllowed := agentProfileAllowsFreshTaskSelectionForMode(profile, task, trustFirst)
			if !profileAllowed {
				if bypass, err := s.agentWorkMayBypassFreshProfileGate(ctx, workspaceID, agentID, task); err != nil {
					return nil, err
				} else if bypass {
					advisoryReasons = append(advisoryReasons, "durable project assignment bypasses fresh-selection profile mismatch")
				} else if agentWorkTaskRequiresStrategyProfile(task) {
					blockReason = "profile_task_mode_mismatch"
					blockSummary = fmt.Sprintf("Agent fresh-selection mode %s is not eligible for strategy/root coordination work.", agentProfileFreshSelectionMode(profile))
				} else if trustFirst && agentWorkTaskIsPureImplementationSelection(task) {
					blockReason = "profile_task_mode_mismatch"
					blockSummary = fmt.Sprintf("Agent fresh-selection mode %s is not eligible for pure implementation work.", agentProfileFreshSelectionMode(profile))
				} else if !trustFirst {
					blockReason = "profile_task_mode_mismatch"
					blockSummary = fmt.Sprintf("Agent fresh-selection mode %s is not eligible for this task.", agentProfileFreshSelectionMode(profile))
				} else {
					advisoryReasons = append(advisoryReasons, fmt.Sprintf("profile mode %s is a weak semantic match", agentProfileFreshSelectionMode(profile)))
				}
			}
		}
		if blockReason == "" && !agentWorkABPCRecoveryActionBypassesProjectRoleLane(task) && !agentWorkTrustFirstMakesRoleLaneAdvisory(trustFirst, task) {
			if ok, err := s.agentMaySelectProjectRoleLaneTask(ctx, workspaceID, agentID, task); err != nil {
				return nil, err
			} else if !ok {
				blockReason = "project_role_lane_required"
				blockSummary = projectClaimRequiredRoleSummary(advisoryRoleTypes)
			}
		} else if blockReason == "" && len(advisoryRoleTypes) > 0 {
			advisoryReasons = append(advisoryReasons, "project role/lane fit is advisory in trust_first frontier mode")
		}
		if blockReason == "" {
			if ok, err := s.agentMaySelectProjectPatchQueueReviewTask(ctx, workspaceID, agentID, task); err != nil {
				return nil, err
			} else if !ok {
				blockReason = "project_patch_queue_review_role_required"
				blockSummary = "Patch queue review tasks still require reviewer or integrator authority."
			}
		}
		if blockReason == "" {
			if packet, blocked, err := s.projectImplementationGateClosed(ctx, workspaceID, task); err != nil {
				return nil, err
			} else if blocked {
				blockReason = "project_gate_closed"
				if packet != nil && packet.Gate != nil {
					blockSummary = strings.TrimSpace(packet.Gate.Summary)
				}
				blockSummary = firstNonEmpty(blockSummary, "Project implementation gate is closed.")
			}
		}
		if blockReason == "" {
			if packet, blocked, err := s.projectValidationArtifactGateClosed(ctx, workspaceID, task); err != nil {
				return nil, err
			} else if blocked {
				blockReason = "project_validation_artifact_missing"
				if packet != nil && packet.Gate != nil {
					blockSummary = strings.TrimSpace(packet.Gate.Summary)
				}
				blockSummary = firstNonEmpty(blockSummary, "Project validation work is waiting for a reviewable artifact.")
			}
		}
		if blockReason == "" {
			if agentWorkTaskBlockedByProductLanePressure(task, productLanePressureProjects, taskProjectByID) {
				blockReason = "product_lane_pressure"
				blockSummary = productLanePressureCoordinationBlockSummary()
			}
		}
		if blockReason == "" {
			if packet, targeted, err := s.projectImplementationFreshClaimRequiresTargetedSwitch(ctx, workspaceID, agentID, task); err != nil {
				return nil, err
			} else if targeted && !trustFirst {
				blockReason = "project_targeted_delegation_required"
				if packet != nil {
					blockSummary = firstNonEmpty(strings.TrimSpace(packet.WhyNow), "Fresh claim requires targeted runtime_switch_task delegation.")
				} else {
					blockSummary = "Fresh claim requires targeted runtime_switch_task delegation."
				}
			} else if targeted {
				advisoryReasons = append(advisoryReasons, "scoped implementer delegation exists, but trust_first treats it as advisory fit evidence")
			}
		}

		fit := agentWorkTaskFitForProfile(profile, task, claimAction, blockReason, advisoryRoleTypes, advisoryReasons)
		taskCopy := task
		candidate := AgentWorkTaskFrontierCandidate{
			Task:           taskCopy,
			Fit:            fit,
			ClaimAction:    claimAction,
			SessionAction:  "start_new",
			Blocked:        blockReason != "",
			BlockReason:    blockReason,
			BlockSummary:   blockSummary,
			AdvisoryReason: strings.Join(uniqueTrimmedAgentWork(advisoryReasons), "; "),
		}
		candidates = append(candidates, candidate)
	}
	suppressAgentWorkIdleReflectionWhenConcreteWorkExists(candidates)
	if len(candidates) == 0 {
		return nil, nil
	}
	hasVisibleCandidate := false
	for _, candidate := range candidates {
		if !candidate.Blocked || agentWorkPressureVisibleBlockedProductFrontierCandidate(candidate, productLanePressureProjects) {
			hasVisibleCandidate = true
			break
		}
	}
	if !hasVisibleCandidate {
		return nil, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.Blocked != right.Blocked {
			return !left.Blocked
		}
		leftPressureProduct := agentWorkTaskProjectUnderProductLanePressure(left.Task, productLanePressureProjects) && agentWorkProductLanePressureCandidate(left.Task)
		rightPressureProduct := agentWorkTaskProjectUnderProductLanePressure(right.Task, productLanePressureProjects) && agentWorkProductLanePressureCandidate(right.Task)
		if leftPressureProduct != rightPressureProduct {
			return leftPressureProduct
		}
		if left.Fit.Score != right.Fit.Score {
			return left.Fit.Score > right.Fit.Score
		}
		if priorityRankForAgentWork(left.Task.Priority) != priorityRankForAgentWork(right.Task.Priority) {
			return priorityRankForAgentWork(left.Task.Priority) < priorityRankForAgentWork(right.Task.Priority)
		}
		return strings.TrimSpace(left.Task.TaskID) < strings.TrimSpace(right.Task.TaskID)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	roster, err := s.buildAgentWorkRoster(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	frontier := AgentWorkTaskFrontier{
		GenerationID:  nextID("frontier"),
		GeneratedAt:   generatedAt,
		SelectionMode: "agent_self_select",
		Summary:       "Autonomous task frontier: inspect fit, peer busyness, hard blocks, and choose/decline with self-fit evidence.",
		Candidates:    candidates,
		Roster:        roster,
	}
	if err := s.recordAgentWorkTaskFrontier(ctx, workspaceID, agentID, frontier); err != nil {
		return nil, err
	}
	return &frontier, nil
}

func suppressAgentWorkIdleReflectionWhenConcreteWorkExists(candidates []AgentWorkTaskFrontierCandidate) {
	concreteByProject := map[string]struct{}{}
	hasWorkspaceConcrete := false
	for _, candidate := range candidates {
		if candidate.Blocked || agentWorkTaskIsProactiveMetacognition(candidate.Task) {
			continue
		}
		projectID := strings.TrimSpace(candidate.Task.ProjectID)
		if projectID == "" {
			hasWorkspaceConcrete = true
			continue
		}
		concreteByProject[projectID] = struct{}{}
	}
	if len(concreteByProject) == 0 && !hasWorkspaceConcrete {
		return
	}
	for i := range candidates {
		if candidates[i].Blocked || !agentWorkTaskIsProactiveMetacognition(candidates[i].Task) {
			continue
		}
		projectID := strings.TrimSpace(candidates[i].Task.ProjectID)
		suppress := hasWorkspaceConcrete
		if projectID != "" {
			_, suppress = concreteByProject[projectID]
		}
		if !suppress {
			continue
		}
		candidates[i].Blocked = true
		candidates[i].BlockReason = "non_idle_work_available"
		candidates[i].BlockSummary = "A concrete non-reflection task is already available in this frontier; claim or decline that work before opening idle reflection."
		candidates[i].Fit.Level = "blocked"
		candidates[i].Fit.Score = 0
		candidates[i].Fit.Reasons = uniqueTrimmedAgentWork(append(candidates[i].Fit.Reasons, "hard block: non_idle_work_available"))
	}
}

func (s *Store) buildAgentWorkRoster(ctx context.Context, workspaceID string) ([]AgentWorkRosterAgent, error) {
	agents, err := s.ListWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	roster := make([]AgentWorkRosterAgent, 0, len(agents))
	for _, agent := range agents {
		profile, err := s.GetAgentProfile(ctx, workspaceID, agent.AgentID)
		if err != nil {
			profile = AgentProfileRecord{}
		}
		profile = agentWorkProfileWithAgentFallback(profile, agent)
		currentTaskIDs := make([]string, 0, len(agent.ActiveTasks)+1)
		for _, active := range agent.ActiveTasks {
			if taskID := strings.TrimSpace(active.TaskID); taskID != "" {
				currentTaskIDs = append(currentTaskIDs, taskID)
			}
		}
		currentSessionID := ""
		if agent.CurrentSession != nil {
			currentSessionID = strings.TrimSpace(agent.CurrentSession.SessionID)
			if taskID := strings.TrimSpace(agent.CurrentSession.TaskID); taskID != "" {
				currentTaskIDs = append(currentTaskIDs, taskID)
			}
		}
		activeTasks := append([]AgentCurrentTask(nil), agent.ActiveTasks...)
		roster = append(roster, AgentWorkRosterAgent{
			AgentID:               strings.TrimSpace(agent.AgentID),
			DisplayName:           strings.TrimSpace(agent.DisplayName),
			Role:                  strings.TrimSpace(agent.Role),
			Status:                strings.TrimSpace(agent.Status),
			IsOnline:              agent.IsOnline,
			LastSeenAt:            agent.LastSeenAt,
			ActiveTaskCount:       len(agent.ActiveTasks),
			CurrentSessionID:      currentSessionID,
			CurrentTaskIDs:        uniqueTrimmedAgentWork(currentTaskIDs),
			Capabilities:          uniqueTrimmedAgentWork(agent.Capabilities),
			ProfileSpecialization: strings.TrimSpace(profile.Specialization),
			ProfileTags:           uniqueTrimmedAgentWork(profile.Tags),
			ToolsAccess:           uniqueTrimmedAgentWork(profile.ToolsAccess),
			Busyness:              agentWorkRosterBusyness(agent),
			ActiveTasks:           activeTasks,
		})
	}
	return roster, nil
}

func agentWorkRosterBusyness(agent AgentRecord) string {
	if !agent.IsOnline {
		return "offline"
	}
	if len(agent.ActiveTasks) >= 2 {
		return "busy"
	}
	if len(agent.ActiveTasks) == 1 {
		return "light"
	}
	if agent.CurrentSession != nil && !isEndedAgentWorkSessionStatus(agent.CurrentSession.Status) {
		return "light"
	}
	return "idle"
}

func agentWorkTaskFitForProfile(profile AgentProfileRecord, task WorkspaceTaskRecord, claimAction, blockReason string, advisoryRoleTypes, advisoryReasons []string) AgentWorkTaskFit {
	requiredModes := agentWorkTaskWorkModes(task)
	preferredSkills := agentWorkTaskPreferredSkills(task)
	preferredTools := agentWorkTaskPreferredTools(task)
	profileMode := agentProfileFreshSelectionMode(profile)
	score := 45
	reasons := make([]string, 0, 8)

	if strings.TrimSpace(claimAction) == "reuse_claim" {
		score += 35
		reasons = append(reasons, "agent already owns this claim")
	}
	if profileMode != "" && containsAgentWorkStringFold(requiredModes, profileMode) {
		score += 30
		reasons = append(reasons, "profile work mode matches task mode")
	} else if profileMode != "" && len(requiredModes) > 0 && !containsAgentWorkStringFold(requiredModes, "general") {
		score -= 25
		reasons = append(reasons, fmt.Sprintf("profile mode %s is not the obvious task mode", profileMode))
	}
	if overlap := intersectAgentWorkStringsFold(agentWorkProfileSignals(profile), preferredSkills); len(overlap) > 0 {
		score += 18
		reasons = append(reasons, "profile signals match task skills: "+strings.Join(overlap, ", "))
	} else if len(preferredSkills) > 0 {
		score -= 20
		reasons = append(reasons, "task skill hints do not match this agent profile")
	}
	if len(preferredTools) > 0 {
		if overlap := intersectAgentWorkStringsFold(profile.ToolsAccess, preferredTools); len(overlap) > 0 {
			score += 14
			reasons = append(reasons, "tool access matches task hints: "+strings.Join(overlap, ", "))
		} else {
			score -= 5
			reasons = append(reasons, "task has tool hints not visible in profile")
		}
	}
	if agentWorkPatchQueueDecisionContinuationTask(task) {
		score += 35
		reasons = append(reasons, "patch queue terminal decision continuation should be materialized before fresh lane work")
	}
	if len(advisoryRoleTypes) > 0 {
		reasons = append(reasons, "project lane advisory roles: "+strings.Join(uniqueTrimmedAgentWork(advisoryRoleTypes), ", "))
	}
	reasons = append(reasons, advisoryReasons...)

	if blockReason != "" {
		reasons = append(reasons, "hard block: "+blockReason)
		return AgentWorkTaskFit{
			Level:              "blocked",
			Score:              0,
			Reasons:            uniqueTrimmedAgentWork(reasons),
			RequiredWorkModes:  requiredModes,
			PreferredWorkModes: requiredModes,
			PreferredSkills:    preferredSkills,
			PreferredTools:     preferredTools,
			AdvisoryRoleTypes:  uniqueTrimmedAgentWork(advisoryRoleTypes),
		}
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	level := "weak_fit"
	switch {
	case score >= 75:
		level = "recommended"
	case score >= 50:
		level = "plausible"
	}
	return AgentWorkTaskFit{
		Level:              level,
		Score:              score,
		Reasons:            uniqueTrimmedAgentWork(reasons),
		RequiredWorkModes:  requiredModes,
		PreferredWorkModes: requiredModes,
		PreferredSkills:    preferredSkills,
		PreferredTools:     preferredTools,
		AdvisoryRoleTypes:  uniqueTrimmedAgentWork(advisoryRoleTypes),
	}
}

func priorityRankForAgentWork(priority string) int {
	switch strings.ToLower(strings.TrimSpace(priority)) {
	case "critical":
		return 0
	case "high":
		return 1
	case "normal":
		return 2
	case "low":
		return 3
	default:
		return 4
	}
}

func agentWorkTaskWorkModes(task WorkspaceTaskRecord) []string {
	modes := append([]string(nil), agentWorkTaskRequirementsStringSlice(task, "required_work_modes", "preferred_work_modes")...)
	if agentWorkTaskLooksReviewScoped(task) {
		modes = append(modes, "review")
	}
	if agentWorkTaskLooksSynthesisScoped(task) {
		modes = append(modes, "synthesis")
	}
	if agentWorkTaskLooksStrategyScoped(task) {
		modes = append(modes, "strategy")
	}
	if agentWorkTaskLooksImplementationScoped(task) {
		modes = append(modes, "implementation")
	}
	if agentWorkTaskLooksValidationScoped(task) {
		modes = append(modes, "validation")
	}
	if len(modes) == 0 {
		modes = append(modes, "general")
	}
	return uniqueTrimmedAgentWork(modes)
}

func agentWorkTaskPreferredSkills(task WorkspaceTaskRecord) []string {
	skills := append([]string(nil), agentWorkTaskRequirementsStringSlice(task, "preferred_skills", "required_skills")...)
	skills = append(skills, task.Tags...)
	skills = append(skills, task.TaskKind, task.TaskTemplate, task.TaskClass, task.ProjectLane)
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	for _, pair := range []struct {
		token string
		skill string
	}{
		{"frontend", "frontend"},
		{"front-end", "frontend"},
		{"ui", "ui"},
		{"ux", "ux"},
		{"visual", "visual-qa"},
		{"browser", "browser"},
		{"design", "design"},
		{"test", "testing"},
		{"review", "review"},
		{"api", "backend"},
		{"server", "backend"},
		{"database", "database"},
	} {
		if agentWorkTextHasSignal(text, pair.token) {
			skills = append(skills, pair.skill)
		}
	}
	return uniqueTrimmedAgentWork(skills)
}

func agentWorkTaskPreferredTools(task WorkspaceTaskRecord) []string {
	tools := agentWorkTaskRequirementsStringSlice(task, "preferred_tools", "required_tools")
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	for _, pair := range []struct {
		token string
		tool  string
	}{
		{"browser", "browser"},
		{"visual", "browser"},
		{"chrome", "chrome-devtools"},
		{"devtools", "chrome-devtools"},
		{"figma", "figma"},
		{"github", "github"},
		{"git", "git"},
		{"slack", "slack"},
	} {
		if agentWorkTextHasSignal(text, pair.token) {
			tools = append(tools, pair.tool)
		}
	}
	return uniqueTrimmedAgentWork(tools)
}

func agentWorkTaskRequirementsStringSlice(task WorkspaceTaskRecord, keys ...string) []string {
	payload := agentWorkTaskRequirementsPayload(task)
	if len(payload) == 0 {
		return nil
	}
	out := make([]string, 0, 8)
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				if str, ok := item.(string); ok {
					out = append(out, str)
				}
			}
		case []string:
			out = append(out, typed...)
		case string:
			out = append(out, typed)
		}
	}
	return uniqueTrimmedAgentWork(out)
}

func agentWorkTaskRequirementBool(task WorkspaceTaskRecord, keys ...string) bool {
	payload := agentWorkTaskRequirementsPayload(task)
	if len(payload) == 0 {
		return false
	}
	for _, key := range keys {
		switch typed := payload[key].(type) {
		case bool:
			if typed {
				return true
			}
		case string:
			switch strings.ToLower(strings.TrimSpace(typed)) {
			case "1", "true", "yes", "y", "on":
				return true
			}
		}
	}
	return false
}

func agentWorkTaskRequirementsPayload(task WorkspaceTaskRecord) map[string]any {
	raw := normalizeTaskRequirementsJSON(task.TaskRequirementsJSON)
	if raw == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return payload
}

func agentWorkProfileSignals(profile AgentProfileRecord) []string {
	signals := append([]string(nil), profile.Tags...)
	signals = append(signals, profile.ToolsAccess...)
	signals = append(signals, profile.Specialization, profile.Bio)
	return uniqueTrimmedAgentWork(signals)
}

func containsAgentWorkStringFold(values []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.ToLower(strings.TrimSpace(value)) == want {
			return true
		}
	}
	return false
}

func intersectAgentWorkStringsFold(left, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	index := map[string]string{}
	for _, value := range left {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		index[strings.ToLower(trimmed)] = trimmed
	}
	out := make([]string, 0, len(right))
	for _, value := range right {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := index[strings.ToLower(trimmed)]; ok {
			out = append(out, trimmed)
			continue
		}
		for _, leftValue := range left {
			if agentWorkSemanticSignalMatches(leftValue, trimmed) {
				out = append(out, trimmed)
				break
			}
		}
	}
	return uniqueTrimmedAgentWork(out)
}

func agentWorkSemanticSignalMatches(profileSignal, taskSignal string) bool {
	left := agentWorkNormalizeSemanticSignal(profileSignal)
	right := agentWorkNormalizeSemanticSignal(taskSignal)
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	leftTokens := agentWorkSemanticSignalTokens(left)
	rightTokens := agentWorkSemanticSignalTokens(right)
	if len(leftTokens) == 0 || len(rightTokens) == 0 {
		return false
	}
	if len(rightTokens) == 1 && len([]rune(rightTokens[0])) < 3 {
		return agentWorkSemanticSignalContainsToken(left, rightTokens[0])
	}
	if len(leftTokens) == 1 && len([]rune(leftTokens[0])) < 3 {
		return agentWorkSemanticSignalContainsToken(right, leftTokens[0])
	}
	rightMatches := agentWorkSemanticSignalMatchedTokenCount(left, rightTokens)
	leftMatches := agentWorkSemanticSignalMatchedTokenCount(right, leftTokens)
	return rightMatches == len(rightTokens) ||
		leftMatches == len(leftTokens) ||
		rightMatches >= 2 ||
		leftMatches >= 2
}

func agentWorkNormalizeSemanticSignal(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer("-", " ", "_", " ", "/", " ", ";", " ", ",", " ", ".", " ", "(", " ", ")", " ")
	value = replacer.Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func agentWorkSemanticSignalTokens(value string) []string {
	return strings.Fields(agentWorkNormalizeSemanticSignal(value))
}

func agentWorkTextHasSignal(text, signal string) bool {
	text = agentWorkNormalizeSemanticSignal(text)
	signal = agentWorkNormalizeSemanticSignal(signal)
	if text == "" || signal == "" {
		return false
	}
	signalTokens := agentWorkSemanticSignalTokens(signal)
	if len(signalTokens) == 0 {
		return false
	}
	for _, token := range signalTokens {
		if !agentWorkSemanticSignalContainsToken(text, token) {
			return false
		}
	}
	return true
}

func agentWorkSemanticSignalMatchedTokenCount(haystack string, tokens []string) int {
	matches := 0
	for _, token := range tokens {
		if strings.TrimSpace(token) == "" {
			continue
		}
		if agentWorkSemanticSignalContainsToken(haystack, token) {
			matches++
		}
	}
	return matches
}

func agentWorkSemanticSignalContainsToken(haystack, token string) bool {
	haystack = agentWorkNormalizeSemanticSignal(haystack)
	token = strings.ToLower(strings.TrimSpace(token))
	if haystack == "" || token == "" {
		return false
	}
	for _, haystackToken := range agentWorkSemanticSignalTokens(haystack) {
		if agentWorkSemanticSignalTokensEquivalent(haystackToken, token) {
			return true
		}
	}
	return false
}

func agentWorkSemanticSignalTokensEquivalent(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	for _, variant := range agentWorkSemanticSignalTokenVariants(left) {
		if variant == right {
			return true
		}
	}
	for _, variant := range agentWorkSemanticSignalTokenVariants(right) {
		if variant == left {
			return true
		}
	}
	return false
}

func agentWorkSemanticSignalTokenVariants(token string) []string {
	token = strings.ToLower(strings.TrimSpace(token))
	explicit := map[string][]string{
		"review":       {"reviewer", "reviewing", "reviews"},
		"reviewer":     {"review"},
		"reviewing":    {"review"},
		"reviews":      {"review"},
		"test":         {"tests", "testing", "tester"},
		"tests":        {"test"},
		"testing":      {"test"},
		"tester":       {"test"},
		"verify":       {"verification", "verifier", "verified", "verifying"},
		"verification": {"verify", "verifier"},
		"verifier":     {"verify", "verification"},
		"validated":    {"validate", "validation"},
		"validating":   {"validate", "validation"},
		"validation":   {"validate", "verify"},
		"validate":     {"validation", "validated", "validating"},
	}
	var out []string
	out = append(out, explicit[token]...)
	if len(token) < 5 {
		return uniqueTrimmedAgentWork(out)
	}
	if strings.HasSuffix(token, "ing") && len(token) > 6 {
		out = append(out, strings.TrimSuffix(token, "ing"))
	}
	if strings.HasSuffix(token, "s") && len(token) > 5 {
		out = append(out, strings.TrimSuffix(token, "s"))
	}
	if strings.HasSuffix(token, "er") && len(token) > 5 {
		out = append(out, strings.TrimSuffix(token, "er"))
	}
	return uniqueTrimmedAgentWork(out)
}

func selectAgentWorkProtoCluster(report ControlReport, taskID, sessionID string, hydration *TaskHydrationBundle) string {
	docKeys := make([]string, 0, 8)
	if hydration != nil {
		for _, doc := range hydration.Docs {
			if key := strings.TrimSpace(doc.DocKey); key != "" {
				docKeys = append(docKeys, key)
			}
		}
	}
	bestID := ""
	bestScore := 0
	for _, cluster := range report.Clusters {
		score := 0
		if containsAgentWorkString(cluster.TaskIDs, taskID) {
			score += 5
		}
		if containsAgentWorkString(cluster.SessionIDs, sessionID) {
			score += 4
		}
		if intersectsAgentWorkStrings(cluster.DocKeys, docKeys) {
			score += 2
		}
		if cluster.ConfirmedTensionCount > 0 || cluster.PendingTensionCount > 0 {
			score++
		}
		clusterID := strings.TrimSpace(cluster.ProtoClusterID)
		if score > bestScore || (score == bestScore && score > 0 && (bestID == "" || strings.Compare(clusterID, bestID) < 0)) {
			bestScore = score
			bestID = clusterID
		}
	}
	return bestID
}

func filterAgentWorkFrontier(items []TensionFrontierItem, protoClusterID string) []TensionFrontierItem {
	if len(items) == 0 {
		return nil
	}
	filtered := make([]TensionFrontierItem, 0, len(items))
	for _, item := range items {
		if protoClusterID != "" && strings.TrimSpace(item.ProtoClusterID) != protoClusterID {
			continue
		}
		filtered = append(filtered, item)
	}
	if len(filtered) == 0 {
		filtered = append(filtered, items...)
	}
	return filtered
}

func uniqueTrimmedAgentWork(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	unique := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		unique = append(unique, trimmed)
	}
	return unique
}

func containsAgentWorkString(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func intersectsAgentWorkStrings(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	index := map[string]struct{}{}
	for _, value := range left {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			index[trimmed] = struct{}{}
		}
	}
	for _, value := range right {
		if _, ok := index[strings.TrimSpace(value)]; ok {
			return true
		}
	}
	return false
}

func isResumableClaimForAgent(task WorkspaceTaskRecord, agentID string) bool {
	if task.ClaimAgentID == nil || strings.TrimSpace(*task.ClaimAgentID) != agentID {
		return false
	}
	if isTerminalTaskStatus(task.Status) {
		return false
	}
	if task.ClaimStatus == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(*task.ClaimStatus)) {
	case model.TaskClaimStatusClaimed:
		return true
	default:
		return false
	}
}

func isRunnableAgentWorkSessionStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case model.SessionStatusBlocked, model.SessionStatusWaitingDecision, model.SessionStatusHandoffPending, model.SessionStatusEnded:
		return false
	default:
		return true
	}
}

func isTerminalTaskStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled:
		return true
	default:
		return false
	}
}

func claimAvailable(status *string) bool {
	if status == nil {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(*status)) {
	case "", model.TaskClaimStatusReleased:
		return true
	default:
		return false
	}
}

func normalizeAgentWorkTrigger(trigger string) string {
	switch strings.ToLower(strings.TrimSpace(trigger)) {
	case "inbound_message":
		return "inbound_message"
	case "runtime_resume":
		return "runtime_resume"
	case "request_resume":
		return "request_resume"
	case "runtime_switch_task":
		return "runtime_switch_task"
	case "runtime_switch_tension":
		return "runtime_switch_tension"
	case "control_switch_task":
		return "control_switch_task"
	case "control_switch_tension":
		return "control_switch_tension"
	case "recovery":
		return "recovery"
	case "system_news":
		return "system_news"
	case "task_project_fields_updated":
		return "task_project_fields_updated"
	default:
		return ""
	}
}

func selectTriggeredActiveLanePublicationResume(taskIndex map[string]WorkspaceTaskRecord, sessions []AgentSessionStateRecord, agentID, trigger string, candidate WorkspaceTaskRecord) (*WorkspaceTaskRecord, *AgentSessionStateRecord, string, bool) {
	if !triggerCanResumeActiveLanePublication(trigger) || !agentWorkTaskLooksActiveLanePublication(candidate) {
		return nil, nil, "", false
	}
	projectID := strings.TrimSpace(candidate.ProjectID)
	if projectID == "" {
		return nil, nil, "", false
	}
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if strings.TrimSpace(session.TaskID) == "" || strings.TrimSpace(session.TaskID) == strings.TrimSpace(candidate.TaskID) {
			continue
		}
		if isEndedAgentWorkSessionStatus(session.Status) {
			continue
		}
		if !isRunnableAgentWorkSessionStatus(session.Status) && !shouldWakeAgentWorkSession(trigger, session.Status) {
			continue
		}
		task, ok := taskIndex[strings.TrimSpace(session.TaskID)]
		if !ok || isTerminalTaskStatus(task.Status) {
			continue
		}
		if strings.TrimSpace(task.ProjectID) != projectID {
			continue
		}
		if !projectTaskRequiresImplementationGate(task) || agentWorkTaskLooksActiveLanePublication(task) {
			continue
		}
		if !isResumableClaimForAgent(task, agentID) {
			continue
		}
		taskCopy := task
		sessionCopy := session
		summary := fmt.Sprintf("Active-lane publication/provenance task %s was rerouted to existing implementation task %s; finish, commit/push, and publish review-ready evidence from the active lane before sidecar publication work.", strings.TrimSpace(candidate.TaskID), strings.TrimSpace(task.TaskID))
		return &taskCopy, &sessionCopy, summary, true
	}
	return nil, nil, "", false
}

func triggerCanResumeActiveLanePublication(trigger string) bool {
	switch strings.TrimSpace(trigger) {
	case "runtime_switch_task", "control_switch_task", "request_resume", "runtime_resume", "task_project_fields_updated":
		return true
	default:
		return false
	}
}

func selectTriggeredAgentWork(taskIndex map[string]WorkspaceTaskRecord, sessions []AgentSessionStateRecord, agentID, trigger, candidateTaskID, candidateSessionID string) (*WorkspaceTaskRecord, *AgentSessionStateRecord, bool) {
	if trigger == "" {
		return nil, nil, false
	}

	suppressedSessionCandidate := false
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != agentID {
			continue
		}
		if candidateSessionID != "" && strings.TrimSpace(session.SessionID) != candidateSessionID {
			continue
		}
		if candidateTaskID != "" && strings.TrimSpace(session.TaskID) != candidateTaskID {
			continue
		}
		task, ok := taskIndex[strings.TrimSpace(session.TaskID)]
		if !ok || isTerminalTaskStatus(task.Status) {
			continue
		}
		if !shouldWakeAgentWorkSession(trigger, session.Status) {
			if !isEndedAgentWorkSessionStatus(session.Status) {
				suppressedSessionCandidate = true
			}
			continue
		}
		taskCopy := task
		sessionCopy := session
		return &taskCopy, &sessionCopy, true
	}

	if suppressedSessionCandidate {
		return nil, nil, false
	}
	if candidateTaskID == "" {
		return nil, nil, false
	}
	task, ok := taskIndex[candidateTaskID]
	if !ok || isTerminalTaskStatus(task.Status) {
		return nil, nil, false
	}
	if isAgentWorkTaskSwitchTrigger(trigger) && claimAvailable(task.ClaimStatus) {
		taskCopy := task
		return &taskCopy, nil, true
	}
	if !isResumableClaimForAgent(task, agentID) && !isExplicitlyWakeableBlockedClaimForAgent(task, agentID, trigger) {
		return nil, nil, false
	}
	taskCopy := task
	return &taskCopy, nil, true
}

func triggeredAgentWorkNoWorkReason(taskIndex map[string]WorkspaceTaskRecord, sessions []AgentSessionStateRecord, agentID, trigger, candidateTaskID, candidateSessionID string) string {
	candidateTaskID = strings.TrimSpace(candidateTaskID)
	if candidateTaskID == "" {
		return "trigger_no_work"
	}
	task, ok := taskIndex[candidateTaskID]
	if !ok {
		return "trigger_task_missing"
	}
	if isTerminalTaskStatus(task.Status) {
		return "trigger_task_terminal"
	}
	suppressedSession := false
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if candidateSessionID != "" && strings.TrimSpace(session.SessionID) != strings.TrimSpace(candidateSessionID) {
			continue
		}
		if strings.TrimSpace(session.TaskID) != candidateTaskID {
			continue
		}
		if !shouldWakeAgentWorkSession(trigger, session.Status) && !isEndedAgentWorkSessionStatus(session.Status) {
			suppressedSession = true
			break
		}
	}
	if suppressedSession {
		return "trigger_session_not_wakeable"
	}
	if task.ClaimAgentID != nil && strings.TrimSpace(*task.ClaimAgentID) != "" && strings.TrimSpace(*task.ClaimAgentID) != strings.TrimSpace(agentID) {
		return "trigger_task_claimed_by_other"
	}
	if !claimAvailable(task.ClaimStatus) && !isResumableClaimForAgent(task, agentID) && !isExplicitlyWakeableBlockedClaimForAgent(task, agentID, trigger) {
		return "trigger_task_not_claimable"
	}
	return "trigger_no_work"
}

func shouldWakeAgentWorkSession(trigger, status string) bool {
	if trigger == "" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", model.SessionStatusActive:
		return true
	case model.SessionStatusBlocked, model.SessionStatusWaitingDecision:
		return trigger == "inbound_message" || trigger == "runtime_resume" || trigger == "request_resume" || trigger == "task_project_fields_updated" || isAgentWorkSwitchTrigger(trigger)
	case model.SessionStatusHandoffPending:
		return trigger == "runtime_resume" || trigger == "request_resume" || trigger == "task_project_fields_updated" || isAgentWorkTaskSwitchTrigger(trigger)
	default:
		return false
	}
}

func isEndedAgentWorkSessionStatus(status string) bool {
	return strings.ToUpper(strings.TrimSpace(status)) == model.SessionStatusEnded
}

func isExplicitlyWakeableBlockedClaimForAgent(task WorkspaceTaskRecord, agentID, trigger string) bool {
	if !isAgentWorkBlockedClaimWakeTrigger(trigger) {
		return false
	}
	if agentWorkStalePatchQueueSupersedeAgentStateEvidenceTask(task) {
		return false
	}
	if projectStrategicLeadCoordinationTask(task) {
		return false
	}
	if task.ClaimAgentID == nil || strings.TrimSpace(*task.ClaimAgentID) != agentID {
		return false
	}
	if isTerminalTaskStatus(task.Status) || task.ClaimStatus == nil {
		return false
	}
	return strings.ToUpper(strings.TrimSpace(*task.ClaimStatus)) == model.TaskClaimStatusBlocked
}

func agentWorkStalePatchQueueSupersedeAgentStateEvidenceTask(task WorkspaceTaskRecord) bool {
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	if !strings.Contains(text, "supersede") {
		return false
	}
	if !(strings.Contains(text, "patch_queue") || strings.Contains(text, "patch queue") || strings.Contains(text, "patchq")) {
		return false
	}
	evidenceDocKey := agentWorkPatchQueueSupersedeEvidenceDocKey(task)
	if evidenceDocKey == "" {
		return false
	}
	return agentWorkPatchQueueSupersedeEvidenceKeyLooksAgentState(evidenceDocKey)
}

func agentWorkPatchQueueSupersedeEvidenceDocKey(task WorkspaceTaskRecord) string {
	text := strings.Join([]string{task.Description, task.Title}, "\n")
	for _, line := range strings.Split(text, "\n") {
		clean := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		lower := strings.ToLower(clean)
		for _, marker := range []string{"evidence_doc_key:", "evidence_doc_key="} {
			if idx := strings.Index(lower, marker); idx >= 0 {
				value := strings.TrimSpace(clean[idx+len(marker):])
				value = strings.Trim(value, "`'\" ")
				fields := strings.Fields(value)
				if len(fields) > 0 {
					return strings.Trim(fields[0], "`'\",.;")
				}
				return value
			}
		}
	}
	return ""
}

func agentWorkPatchQueueSupersedeEvidenceKeyLooksAgentState(docKey string) bool {
	key := strings.ToLower(strings.TrimSpace(docKey))
	if key == "" {
		return false
	}
	if key == "claimed_work" || key == "current_context" {
		return true
	}
	return strings.Contains(key, ".claimed_work") || strings.Contains(key, ".current_context") ||
		strings.Contains(key, "claimed_work") || strings.Contains(key, "current_context")
}

func isAgentWorkBlockedClaimWakeTrigger(trigger string) bool {
	switch strings.TrimSpace(trigger) {
	case "inbound_message", "runtime_resume", "request_resume", "task_project_fields_updated":
		return true
	default:
		return isAgentWorkTaskSwitchTrigger(trigger)
	}
}

func isAgentWorkSwitchTrigger(trigger string) bool {
	return isAgentWorkTaskSwitchTrigger(trigger) || isAgentWorkTensionSwitchTrigger(trigger)
}

func isAgentWorkTaskSwitchTrigger(trigger string) bool {
	switch strings.TrimSpace(trigger) {
	case "runtime_switch_task", "control_switch_task":
		return true
	default:
		return false
	}
}

func isAgentWorkTensionSwitchTrigger(trigger string) bool {
	switch strings.TrimSpace(trigger) {
	case "runtime_switch_tension", "control_switch_tension":
		return true
	default:
		return false
	}
}

func reasonForAgentWorkSelection(session *AgentSessionStateRecord) string {
	if session == nil {
		return "resume_claim"
	}
	return "resume_session"
}

func claimActionForAgentWork(task WorkspaceTaskRecord, agentID string) string {
	if isResumableClaimForAgent(task, agentID) {
		return "reuse_claim"
	}
	return "claim_required"
}

func sessionActionForAgentWork(session *AgentSessionStateRecord, trigger string) string {
	if session == nil {
		return "start_new"
	}
	if isRunnableAgentWorkSessionStatus(session.Status) {
		return "reuse_active"
	}
	if trigger != "" && shouldWakeAgentWorkSession(trigger, session.Status) {
		return "resume_inactive"
	}
	return "start_new"
}

func resumeSummaryForAgentWork(session *AgentSessionStateRecord, trigger string) string {
	if session == nil || isRunnableAgentWorkSessionStatus(session.Status) {
		return ""
	}
	switch strings.ToUpper(strings.TrimSpace(session.Status)) {
	case model.SessionStatusWaitingDecision:
		if trigger == "task_project_fields_updated" {
			return "Task project fields changed; refresh decision context"
		}
		if trigger == "inbound_message" {
			return "Decision context changed after inbound message; resume session"
		}
		return "Decision context changed; resume session"
	case model.SessionStatusBlocked:
		if trigger == "task_project_fields_updated" {
			return "Task project fields changed; refresh blocked session context"
		}
		if trigger == "inbound_message" {
			return "Blocked session received new inbound context; resume session"
		}
		return "Blocked session requested for resume"
	case model.SessionStatusHandoffPending:
		if trigger == "task_project_fields_updated" {
			return "Task project fields changed; refresh handoff context"
		}
		return "Resume requested for handoff-pending session"
	default:
		if trigger == "task_project_fields_updated" {
			return "Task project fields changed; refresh session context"
		}
		if trigger == "system_news" {
			return "System news published; refresh session context"
		}
		return "Resume selected session"
	}
}

func isClaimOwnedByAgentWork(task WorkspaceTaskRecord, agentID string) bool {
	return task.ClaimAgentID != nil && strings.TrimSpace(*task.ClaimAgentID) == strings.TrimSpace(agentID)
}
