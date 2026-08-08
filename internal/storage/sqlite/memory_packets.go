package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	memoryPacketKindKernel          = "KERNEL"
	memoryPacketKindShell           = "SHELL"
	memoryPacketContract            = "memory_packet.fresh_context.v1"
	memoryPacketSchemaVersion       = "rmp-1.2/phase6"
	memoryPacketDefaultUpdates      = 8
	memoryPacketDefaultArtifacts    = 8
	memoryPacketDefaultRelated      = 6
	memoryPacketDefaultClaims       = 24
	memoryPacketDefaultPacks        = 6
	memoryPacketDefaultEvents       = 12
	memoryPacketDefaultFrontier     = 4
	memoryPacketDefaultCoordination = 6
	memoryPacketDefaultOpenQueues   = 10
	memoryPacketDefaultShellLoops   = 12
	memoryPacketDefaultCoordFloor   = 1
	memoryPacketDefaultDissentQ     = 1
	memoryPacketAdaptiveCoordBump   = 1
	memoryPacketAdaptiveCoordSignal = 10
)

const (
	MemoryRetrievalLaneDeterministic = "DETERMINISTIC"
	MemoryRetrievalLaneCluster       = "CLUSTER"
	MemoryRetrievalLaneSemantic      = "SEMANTIC"
	MemoryRetrievalLaneContrastive   = "CONTRASTIVE"
	MemoryRetrievalLaneCoordination  = "COORDINATION"
	MemoryRetrievalLaneProcedural    = "PROCEDURAL"
	MemoryRetrievalLaneBridge        = "BRIDGE"
	MemoryRetrievalLaneIdentity      = "IDENTITY"
)

type MemoryPacketLaneBudget struct {
	ItemLimit int `json:"item_limit"`
}

type MemoryPacketBudget struct {
	Lanes             map[string]MemoryPacketLaneBudget `json:"lanes"`
	CoordinationFloor int                               `json:"coordination_floor,omitempty"`
	DissentQuota      int                               `json:"dissent_quota,omitempty"`
}

func defaultMemoryPacketBudget() *MemoryPacketBudget {
	return &MemoryPacketBudget{
		CoordinationFloor: memoryPacketDefaultCoordFloor,
		DissentQuota:      memoryPacketDefaultDissentQ,
		Lanes: map[string]MemoryPacketLaneBudget{
			MemoryRetrievalLaneDeterministic: {ItemLimit: memoryPacketDefaultUpdates},
			MemoryRetrievalLaneCluster:       {ItemLimit: memoryPacketDefaultFrontier},
			MemoryRetrievalLaneSemantic:      {ItemLimit: memoryPacketDefaultClaims},
			MemoryRetrievalLaneContrastive:   {ItemLimit: memoryPacketDefaultClaims / 2},
			MemoryRetrievalLaneCoordination:  {ItemLimit: memoryPacketDefaultCoordination},
			MemoryRetrievalLaneProcedural:    {ItemLimit: memoryPacketDefaultRelated},
			MemoryRetrievalLaneBridge:        {ItemLimit: memoryPacketDefaultPacks},
			MemoryRetrievalLaneIdentity:      {ItemLimit: 2},
		},
	}
}

type MemoryPacketFilter struct {
	WorkspaceID    string              `json:"workspace_id"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	AgentID        string              `json:"agent_id,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	IncludeAllDocs bool                `json:"include_all_docs,omitempty"`
	Budget         *MemoryPacketBudget `json:"budget,omitempty"`
}

type MemoryPacketMeta struct {
	Contract       string `json:"contract,omitempty"`
	PacketKind     string `json:"packet_kind"`
	PacketKey      string `json:"packet_key"`
	BasisDigest    string `json:"basis_digest"`
	SchemaVersion  string `json:"schema_version"`
	GeneratedAt    string `json:"generated_at"`
	WorkspaceID    string `json:"workspace_id"`
	TaskID         string `json:"task_id"`
	SessionID      string `json:"session_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
	Scope          string `json:"scope"`
}

type MemoryPacketReference struct {
	PacketKey      string `json:"packet_key"`
	BasisDigest    string `json:"basis_digest"`
	ProtoClusterID string `json:"proto_cluster_id,omitempty"`
}

type MemoryPacketBasisRef struct {
	RefKind      string `json:"ref_kind"`
	RefID        string `json:"ref_id"`
	Role         string `json:"role,omitempty"`
	VersionToken string `json:"version_token,omitempty"`
}

type MemoryPacketBasisSummary struct {
	TotalRefCount           int `json:"total_ref_count"`
	RuntimeEventRefCount    int `json:"runtime_event_ref_count"`
	EpisodePackRefCount     int `json:"episode_pack_ref_count"`
	KnowledgeClaimRefCount  int `json:"knowledge_claim_ref_count"`
	WorkspaceMemoryRefCount int `json:"workspace_memory_ref_count"`
	CoordinationBasisCount  int `json:"coordination_basis_count"`
	DifferentialBasisCount  int `json:"differential_basis_count"`
	ProceduralBasisCount    int `json:"procedural_basis_count"`
	IdentityBasisCount      int `json:"identity_basis_count"`
	RecentTraceBasisCount   int `json:"recent_trace_basis_count"`
}

type MemoryPacketBoundarySummary struct {
	HardConstraintCount            int `json:"hard_constraint_count"`
	AcceptedDecisionCount          int `json:"accepted_decision_count"`
	DecisionRecordCount            int `json:"decision_record_count"`
	ActiveBlockerCount             int `json:"active_blocker_count"`
	BlockerHypothesisCount         int `json:"blocker_hypothesis_count"`
	DissentClaimCount              int `json:"dissent_claim_count"`
	ArchivedDissentClaimCount      int `json:"archived_dissent_claim_count"`
	AlternativeBranchCount         int `json:"alternative_branch_count"`
	ArchivedAlternativeBranchCount int `json:"archived_alternative_branch_count"`
	ProceduralClaimCount           int `json:"procedural_claim_count"`
	IdentityMemoryCount            int `json:"identity_memory_count"`
	TraceContextCount              int `json:"trace_context_count"`
}

type MemoryPacketTaskCharter struct {
	Title               string `json:"title,omitempty"`
	Description         string `json:"description,omitempty"`
	Priority            string `json:"priority,omitempty"`
	Status              string `json:"status,omitempty"`
	TaskKind            string `json:"task_kind,omitempty"`
	TaskTemplate        string `json:"task_template,omitempty"`
	TaskClass           string `json:"task_class,omitempty"`
	TaskClassSource     string `json:"task_class_source,omitempty"`
	TaskClassUpdatedAt  string `json:"task_class_updated_at,omitempty"`
	ProjectID           string `json:"project_id,omitempty"`
	ProjectLane         string `json:"project_lane,omitempty"`
	RequiresProjectGate bool   `json:"requires_project_gate,omitempty"`
}

type MemoryPacketClusterContext struct {
	Resolved             bool                  `json:"resolved"`
	ProtoClusterID       string                `json:"proto_cluster_id,omitempty"`
	Frontier             []TensionFrontierItem `json:"frontier,omitempty"`
	DominantTension      *TensionFrontierItem  `json:"dominant_tension,omitempty"`
	CriticalAmbiguities  []TensionFrontierItem `json:"critical_ambiguities,omitempty"`
	ActiveContradictions []TensionFrontierItem `json:"active_contradictions,omitempty"`
	RelatedSegmentRefs   []string              `json:"related_segment_refs,omitempty"`
}

type MemoryPacketKernelCoordination struct {
	HardConstraints      []KnowledgeClaimRecord `json:"hard_constraints,omitempty"`
	AcceptedDecisions    []KnowledgeClaimRecord `json:"accepted_decisions,omitempty"`
	DecisionRecords      []KnowledgeClaimRecord `json:"decision_records,omitempty"`
	ActiveBlockers       []KnowledgeClaimRecord `json:"active_blockers,omitempty"`
	BlockerSymptoms      []KnowledgeClaimRecord `json:"blocker_symptoms,omitempty"`
	BlockerHypotheses    []KnowledgeClaimRecord `json:"blocker_hypotheses,omitempty"`
	BeliefUpdates        []RSPBeliefClaimReport `json:"belief_updates,omitempty"`
	OpenQueues           []OperatorQueueRecord  `json:"open_queues,omitempty"`
	CriticalAmbiguities  []TensionFrontierItem  `json:"critical_ambiguities,omitempty"`
	ActiveContradictions []TensionFrontierItem  `json:"active_contradictions,omitempty"`
	LastVerifiedHandoff  *EpisodePackRecord     `json:"last_verified_handoff,omitempty"`
}

type MemoryKernelPacket struct {
	Meta            MemoryPacketMeta                `json:"meta"`
	Task            TaskStatus                      `json:"task"`
	WorkspaceTask   *WorkspaceTaskRecord            `json:"workspace_task,omitempty"`
	TaskCharter     MemoryPacketTaskCharter         `json:"task_charter"`
	Docs            []WorkspaceDocRecord            `json:"docs,omitempty"`
	Artifacts       []WorkspaceArtifactRecord       `json:"artifacts,omitempty"`
	TaskLinks       []WorkspaceTaskLinkRecord       `json:"task_links,omitempty"`
	RelatedTasks    []TaskStatus                    `json:"related_tasks,omitempty"`
	SegmentReport   *WorkspaceSegmentReport         `json:"segment_report,omitempty"`
	Coordination    MemoryPacketKernelCoordination  `json:"coordination"`
	Cluster         MemoryPacketClusterContext      `json:"cluster"`
	BoundarySummary *MemoryPacketBoundarySummary    `json:"boundary_summary,omitempty"`
	BasisSummary    *MemoryPacketBasisSummary       `json:"basis_summary,omitempty"`
	ClaimFreshness  *KnowledgeClaimFreshnessSummary `json:"claim_freshness,omitempty"`
	BasisRefs       []MemoryPacketBasisRef          `json:"basis_refs,omitempty"`
}

type MemoryShellPacket struct {
	Meta                MemoryPacketMeta                `json:"meta"`
	KernelRef           MemoryPacketReference           `json:"kernel_ref"`
	Session             *AgentSessionRecord             `json:"session,omitempty"`
	SessionState        *AgentSessionStateRecord        `json:"session_state,omitempty"`
	StateEstimate       *RSPStateReport                 `json:"state_estimate,omitempty"`
	FocusSummary        string                          `json:"focus_summary,omitempty"`
	RecentUpdates       []AgentUpdateRecord             `json:"recent_updates,omitempty"`
	RecentEpisodePacks  []EpisodePackRecord             `json:"recent_episode_packs,omitempty"`
	RecentRuntimeEvents []RuntimeEventRecord            `json:"recent_runtime_events,omitempty"`
	IdentityMemories    []WorkspaceMemoryRecord         `json:"identity_memories,omitempty"`
	ProceduralClaims    []KnowledgeClaimRecord          `json:"procedural_claims,omitempty"`
	DifferentialClaims  []KnowledgeClaimRecord          `json:"differential_claims,omitempty"`
	OpenLoops           []string                        `json:"open_loops,omitempty"`
	RepairChains        []string                        `json:"repair_chains,omitempty"`
	RelatedSegmentRefs  []string                        `json:"related_segment_refs,omitempty"`
	BoundarySummary     *MemoryPacketBoundarySummary    `json:"boundary_summary,omitempty"`
	BasisSummary        *MemoryPacketBasisSummary       `json:"basis_summary,omitempty"`
	ClaimFreshness      *KnowledgeClaimFreshnessSummary `json:"claim_freshness,omitempty"`
	BasisRefs           []MemoryPacketBasisRef          `json:"basis_refs,omitempty"`
}

type memoryPacketBuildContext struct {
	filter         MemoryPacketFilter
	workspaceID    string
	taskID         string
	sessionID      string
	agentID        string
	coordExplicit  bool
	bridgeExplicit bool
	hydration      TaskHydrationBundle
	locus          InstrumentationLocusBundle
	claims         []KnowledgeClaimRecord
	identity       []WorkspaceMemoryRecord
	procedural     []KnowledgeClaimRecord
	coordination   []KnowledgeClaimRecord
	episodePacks   []EpisodePackRecord
	runtimeEvents  []RuntimeEventRecord
	openQueues     []OperatorQueueRecord
	session        *AgentSessionRecord
	sessionState   *AgentSessionStateRecord
	timeAuthority  WorkspaceTimeAuthority
	referenceAt    string
	claimFreshness *KnowledgeClaimFreshnessSummary
	docKeys        []string
	artifactRefs   []string
}

func normalizeMemoryPacketFilter(filter MemoryPacketFilter) MemoryPacketFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.DocKeys = uniqueTrimmedLocusStrings(filter.DocKeys)
	filter.ArtifactRefs = uniqueTrimmedLocusStrings(filter.ArtifactRefs)

	if filter.Budget == nil {
		filter.Budget = defaultMemoryPacketBudget()
	}
	if filter.Budget.Lanes == nil {
		filter.Budget.Lanes = make(map[string]MemoryPacketLaneBudget)
	}
	defaults := defaultMemoryPacketBudget()
	for lane, baseline := range defaults.Lanes {
		if current, ok := filter.Budget.Lanes[lane]; !ok || current.ItemLimit <= 0 {
			filter.Budget.Lanes[lane] = baseline
		}
	}
	if filter.Budget.CoordinationFloor <= 0 {
		filter.Budget.CoordinationFloor = defaults.CoordinationFloor
	}
	if filter.Budget.DissentQuota <= 0 {
		filter.Budget.DissentQuota = defaults.DissentQuota
	}
	return filter
}

func buildMemoryPacketBoundarySummaryForKernel(packet MemoryKernelPacket) *MemoryPacketBoundarySummary {
	return &MemoryPacketBoundarySummary{
		HardConstraintCount:    len(packet.Coordination.HardConstraints),
		AcceptedDecisionCount:  len(packet.Coordination.AcceptedDecisions),
		DecisionRecordCount:    len(packet.Coordination.DecisionRecords),
		ActiveBlockerCount:     len(packet.Coordination.ActiveBlockers),
		BlockerHypothesisCount: len(packet.Coordination.BlockerHypotheses),
	}
}

func buildMemoryPacketBoundarySummaryForShell(packet MemoryShellPacket) *MemoryPacketBoundarySummary {
	summary := &MemoryPacketBoundarySummary{
		ProceduralClaimCount: len(packet.ProceduralClaims),
		IdentityMemoryCount:  len(packet.IdentityMemories),
		TraceContextCount:    len(packet.RecentEpisodePacks) + len(packet.RecentRuntimeEvents),
	}
	for _, claim := range packet.DifferentialClaims {
		switch strings.TrimSpace(claim.ClaimType) {
		case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT":
			summary.DissentClaimCount++
			if claim.ArchivedAt != nil {
				summary.ArchivedDissentClaimCount++
			}
		case "ALTERNATIVE_BRANCH":
			summary.AlternativeBranchCount++
			if claim.ArchivedAt != nil {
				summary.ArchivedAlternativeBranchCount++
			}
		}
	}
	return summary
}

func buildMemoryPacketBasisSummary(basisRefs []MemoryPacketBasisRef) *MemoryPacketBasisSummary {
	summary := &MemoryPacketBasisSummary{
		TotalRefCount: len(basisRefs),
	}
	for _, ref := range basisRefs {
		switch strings.TrimSpace(ref.RefKind) {
		case "runtime_event":
			summary.RuntimeEventRefCount++
		case "episode_pack":
			summary.EpisodePackRefCount++
		case "knowledge_claim":
			summary.KnowledgeClaimRefCount++
		case "workspace_memory":
			summary.WorkspaceMemoryRefCount++
		}
		switch strings.TrimSpace(ref.Role) {
		case "hard_constraint", "accepted_decision", "active_blocker", "blocker_hypothesis":
			summary.CoordinationBasisCount++
		case "differential_claim":
			summary.DifferentialBasisCount++
		case "procedural_claim":
			summary.ProceduralBasisCount++
		case "identity_memory_task", "identity_memory_session", "identity_memory_workspace":
			summary.IdentityBasisCount++
		case "recent_episode_pack", "recent_runtime_event":
			summary.RecentTraceBasisCount++
		}
	}
	return summary
}

func (s *Store) BuildMemoryKernelPacket(ctx context.Context, filter MemoryPacketFilter) (MemoryKernelPacket, error) {
	packetCtx, err := s.buildMemoryPacketContext(ctx, filter, false)
	if err != nil {
		return MemoryKernelPacket{}, err
	}
	packet := s.buildKernelPacketFromContext(ctx, packetCtx)
	packet.BoundarySummary = buildMemoryPacketBoundarySummaryForKernel(packet)
	packet.BasisSummary = buildMemoryPacketBasisSummary(packet.BasisRefs)
	return packet, nil
}

func (s *Store) BuildMemoryShellPacket(ctx context.Context, filter MemoryPacketFilter) (MemoryShellPacket, error) {
	packetCtx, err := s.buildMemoryPacketContext(ctx, filter, true)
	if err != nil {
		return MemoryShellPacket{}, err
	}
	kernel := s.buildKernelPacketFromContext(ctx, packetCtx)
	now := packetCtx.referenceAt
	if strings.TrimSpace(now) == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	stateEstimate := s.buildRSPStateEstimateForPacket(packetCtx)

	basisRefs := append([]MemoryPacketBasisRef(nil), kernel.BasisRefs...)
	for _, record := range packetCtx.hydration.Updates {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "agent_update", record.UpdateID, "recent_update", record.CreatedAt)
	}
	for _, record := range packetCtx.episodePacks {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "episode_pack", record.PackID, "recent_episode_pack", record.UpdatedAt)
	}
	for _, record := range packetCtx.runtimeEvents {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "runtime_event", record.EventID, "recent_runtime_event", record.CreatedAt)
	}
	for _, record := range packetCtx.claims {
		if isMemoryPacketDifferentialClaim(record) {
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "differential_claim", memoryPacketClaimVersionToken(record))
		}
	}
	for _, record := range packetCtx.identity {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "workspace_memory", record.MemoryID, memoryPacketIdentityBasisRole(record), record.UpdatedAt)
	}
	for _, record := range packetCtx.procedural {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "procedural_claim", memoryPacketClaimVersionToken(record))
	}
	if packetCtx.session != nil {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "agent_session", packetCtx.session.SessionID, "session", packetCtx.session.CreatedAt)
	}
	if packetCtx.sessionState != nil {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "session_state", packetCtx.sessionState.SessionID, "session_state", packetCtx.sessionState.UpdatedAt)
	}
	basisRefs = appendMemoryPacketRSPStateBasisRefs(basisRefs, packetCtx.locus)

	openLoops, repairChains := buildMemoryPacketShellLoopSets(packetCtx.episodePacks)
	packet := MemoryShellPacket{
		Meta: MemoryPacketMeta{
			Contract:       memoryPacketContract,
			PacketKind:     memoryPacketKindShell,
			PacketKey:      buildMemoryPacketKey(memoryPacketKindShell, packetCtx.workspaceID, packetCtx.taskID, packetCtx.sessionID, packetCtx.agentID),
			BasisDigest:    buildMemoryPacketBasisDigest(basisRefs),
			SchemaVersion:  memoryPacketSchemaVersion,
			GeneratedAt:    now,
			WorkspaceID:    packetCtx.workspaceID,
			TaskID:         packetCtx.taskID,
			SessionID:      packetCtx.sessionID,
			AgentID:        packetCtx.agentID,
			ProtoClusterID: packetCtx.locus.ProtoClusterID,
			Scope:          buildMemoryPacketScope(packetCtx.taskID, packetCtx.sessionID, packetCtx.agentID),
		},
		KernelRef: MemoryPacketReference{
			PacketKey:      kernel.Meta.PacketKey,
			BasisDigest:    kernel.Meta.BasisDigest,
			ProtoClusterID: kernel.Meta.ProtoClusterID,
		},
		Session:             packetCtx.session,
		SessionState:        packetCtx.sessionState,
		StateEstimate:       stateEstimate,
		FocusSummary:        resolveMemoryPacketFocusSummary(packetCtx),
		RecentUpdates:       append([]AgentUpdateRecord(nil), packetCtx.hydration.Updates...),
		RecentEpisodePacks:  append([]EpisodePackRecord(nil), packetCtx.episodePacks...),
		RecentRuntimeEvents: append([]RuntimeEventRecord(nil), packetCtx.runtimeEvents...),
		IdentityMemories:    append([]WorkspaceMemoryRecord(nil), packetCtx.identity...),
		ProceduralClaims:    append([]KnowledgeClaimRecord(nil), packetCtx.procedural...),
		DifferentialClaims: collectMemoryPacketDifferentialClaimsWithBudget(
			packetCtx.claims,
			memoryPacketLaneLimit(packetCtx.filter.Budget, MemoryRetrievalLaneContrastive),
			memoryPacketDissentQuota(packetCtx.filter.Budget, MemoryRetrievalLaneContrastive),
		),
		OpenLoops:          openLoops,
		RepairChains:       repairChains,
		RelatedSegmentRefs: append([]string(nil), packetCtx.locus.RelatedSegmentRefs...),
		ClaimFreshness:     cloneKnowledgeClaimFreshnessSummary(packetCtx.claimFreshness),
		BasisRefs:          basisRefs,
	}
	packet.BoundarySummary = buildMemoryPacketBoundarySummaryForShell(packet)
	packet.BasisSummary = buildMemoryPacketBasisSummary(packet.BasisRefs)
	return packet, nil
}

func (s *Store) buildMemoryPacketContext(ctx context.Context, filter MemoryPacketFilter, requireAgent bool) (memoryPacketBuildContext, error) {
	coordExplicit := memoryPacketHasExplicitLane(filter.Budget, MemoryRetrievalLaneCoordination)
	bridgeExplicit := memoryPacketHasExplicitLane(filter.Budget, MemoryRetrievalLaneBridge)
	filter = normalizeMemoryPacketFilter(filter)
	if filter.WorkspaceID == "" {
		return memoryPacketBuildContext{}, errors.New("workspace_id is required")
	}
	if filter.TaskID == "" && filter.SessionID == "" {
		return memoryPacketBuildContext{}, errors.New("task_id or session_id is required")
	}
	if requireAgent && filter.AgentID == "" {
		return memoryPacketBuildContext{}, errors.New("agent_id is required")
	}

	packetCtx := memoryPacketBuildContext{
		filter:         filter,
		workspaceID:    filter.WorkspaceID,
		taskID:         filter.TaskID,
		sessionID:      filter.SessionID,
		agentID:        filter.AgentID,
		coordExplicit:  coordExplicit,
		bridgeExplicit: bridgeExplicit,
	}
	timeAuthority, err := s.GetWorkspaceTimeAuthority(ctx, packetCtx.workspaceID)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.timeAuthority = timeAuthority
	packetCtx.referenceAt = generatedAtFromWorkspaceTimeAuthority(timeAuthority)
	claimFreshness, err := s.BuildKnowledgeClaimFreshnessSummary(ctx, packetCtx.workspaceID, packetCtx.referenceAt, knowledgeClaimFreshnessMaxItems)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.claimFreshness = &claimFreshness
	if filter.SessionID != "" {
		session, err := s.GetAgentSession(ctx, filter.SessionID)
		if err != nil {
			return memoryPacketBuildContext{}, err
		}
		if strings.TrimSpace(session.WorkspaceID) != filter.WorkspaceID {
			return memoryPacketBuildContext{}, fmt.Errorf("session %s does not belong to workspace %s", filter.SessionID, filter.WorkspaceID)
		}
		packetCtx.session = session
		if filter.TaskID == "" {
			packetCtx.taskID = strings.TrimSpace(session.TaskID)
		} else if strings.TrimSpace(session.TaskID) != "" && strings.TrimSpace(session.TaskID) != filter.TaskID {
			return memoryPacketBuildContext{}, fmt.Errorf("session %s does not belong to task %s", filter.SessionID, filter.TaskID)
		}
		if requireAgent && strings.TrimSpace(session.AgentID) != "" && strings.TrimSpace(session.AgentID) != filter.AgentID {
			return memoryPacketBuildContext{}, fmt.Errorf("session %s does not belong to agent %s", filter.SessionID, filter.AgentID)
		}
		state, err := s.GetAgentSessionState(ctx, filter.WorkspaceID, filter.SessionID)
		if err != nil {
			return memoryPacketBuildContext{}, err
		}
		packetCtx.sessionState = &state
		if packetCtx.taskID == "" {
			packetCtx.taskID = strings.TrimSpace(state.TaskID)
		}
	}
	if packetCtx.taskID == "" {
		return memoryPacketBuildContext{}, errors.New("task_id is required after session resolution")
	}

	selectedDocKeys := append([]string{}, filter.DocKeys...)
	artifactRefs := append([]string{}, filter.ArtifactRefs...)
	if packetCtx.sessionState != nil {
		selectedDocKeys = append(selectedDocKeys, packetCtx.sessionState.RelatedDocKeys...)
		for _, ref := range packetCtx.sessionState.RelatedArtifactRefs {
			if trimmed := strings.TrimSpace(ref.Ref); trimmed != "" {
				artifactRefs = append(artifactRefs, trimmed)
			}
		}
	}
	selectedDocKeys = uniqueTrimmedLocusStrings(selectedDocKeys)
	artifactRefs = uniqueTrimmedLocusStrings(artifactRefs)

	budget := packetCtx.filter.Budget
	hydration, err := s.GetTaskHydrationBundle(ctx, TaskHydrationFilter{
		TaskID:           packetCtx.taskID,
		WorkspaceID:      packetCtx.workspaceID,
		DocKeys:          selectedDocKeys,
		IncludeAllDocs:   filter.IncludeAllDocs,
		UpdatesLimit:     budget.Lanes[MemoryRetrievalLaneDeterministic].ItemLimit,
		ArtifactLimit:    budget.Lanes[MemoryRetrievalLaneDeterministic].ItemLimit,
		RelatedTaskLimit: budget.Lanes[MemoryRetrievalLaneProcedural].ItemLimit,
	})
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	if !filter.IncludeAllDocs {
		docs, err := s.listMemoryPacketSelectedDocs(ctx, packetCtx.workspaceID, selectedDocKeys)
		if err != nil {
			return memoryPacketBuildContext{}, err
		}
		hydration.Docs = docs
	}
	packetCtx.hydration = hydration

	for _, record := range hydration.Artifacts {
		if trimmed := strings.TrimSpace(record.ArtifactRef); trimmed != "" {
			artifactRefs = append(artifactRefs, trimmed)
		}
	}
	packetCtx.docKeys = collectMemoryPacketDocKeys(hydration.Docs, selectedDocKeys)
	packetCtx.artifactRefs = uniqueTrimmedLocusStrings(artifactRefs)

	locus, err := s.BuildInstrumentationLocusBundle(ctx, InstrumentationLocusFilter{
		WorkspaceID:   packetCtx.workspaceID,
		AgentID:       filter.AgentID,
		TaskID:        packetCtx.taskID,
		SessionID:     packetCtx.sessionID,
		DocKeys:       append([]string(nil), packetCtx.docKeys...),
		ArtifactRefs:  append([]string(nil), packetCtx.artifactRefs...),
		FrontierLimit: memoryPacketLaneLimit(budget, MemoryRetrievalLaneCluster),
	})
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.locus = locus

	claims, err := s.listMemoryPacketClaims(
		ctx,
		packetCtx.workspaceID,
		packetCtx.taskID,
		packetCtx.sessionID,
		packetCtx.referenceAt,
		memoryPacketLaneLimit(budget, MemoryRetrievalLaneSemantic),
		memoryPacketLaneLimit(budget, MemoryRetrievalLaneContrastive),
		memoryPacketDissentQuota(budget, MemoryRetrievalLaneContrastive),
	)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.claims = claims
	packetCtx.identity, err = s.listMemoryPacketIdentityMemories(
		ctx,
		packetCtx.workspaceID,
		packetCtx.taskID,
		packetCtx.sessionID,
		packetCtx.agentID,
		memoryPacketLaneLimit(budget, MemoryRetrievalLaneIdentity),
	)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.procedural, err = s.listMemoryPacketProceduralClaims(
		ctx,
		packetCtx.workspaceID,
		packetCtx.taskID,
		packetCtx.sessionID,
		packetCtx.referenceAt,
		memoryPacketLaneLimit(budget, MemoryRetrievalLaneProcedural),
	)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	openQueues, err := s.ListOperatorQueueItems(ctx, OperatorQueueFilter{
		WorkspaceID: packetCtx.workspaceID,
		Status:      "OPEN",
		TaskID:      packetCtx.taskID,
		SessionID:   packetCtx.sessionID,
		Limit:       memoryPacketDefaultOpenQueues,
	})
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.openQueues = openQueues

	packetCtx.coordination, err = s.listMemoryPacketKernelCoordinationClaims(
		ctx,
		packetCtx.workspaceID,
		packetCtx.taskID,
		packetCtx.sessionID,
		packetCtx.referenceAt,
		memoryPacketAdaptiveCoordinationLimit(budget, packetCtx.locus, packetCtx.openQueues, packetCtx.coordExplicit),
	)
	if err != nil {
		return memoryPacketBuildContext{}, err
	}

	packs, err := s.ListEpisodePacks(ctx, EpisodePackFilter{
		WorkspaceID: packetCtx.workspaceID,
		TaskID:      packetCtx.taskID,
		SessionID:   packetCtx.sessionID,
		Limit:       memoryPacketAdaptiveBridgeLimit(budget, packetCtx.locus, packetCtx.bridgeExplicit),
	})
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.episodePacks = packs

	events, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID:      packetCtx.workspaceID,
		TaskID:           packetCtx.taskID,
		SessionID:        packetCtx.sessionID,
		ExcludeSynthetic: true,
		Limit:            budget.Lanes[MemoryRetrievalLaneDeterministic].ItemLimit,
	})
	if err != nil {
		return memoryPacketBuildContext{}, err
	}
	packetCtx.runtimeEvents = events

	return packetCtx, nil
}

func (s *Store) buildKernelPacketFromContext(ctx context.Context, packetCtx memoryPacketBuildContext) MemoryKernelPacket {
	now := packetCtx.referenceAt
	if strings.TrimSpace(now) == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	cluster := buildMemoryPacketClusterContext(packetCtx.locus)
	kernelClaims := mergeMemoryPacketClaims(packetCtx.coordination, packetCtx.claims)
	hardConstraints, decisions, blockerSymptoms, blockerHypotheses := splitMemoryPacketKernelClaims(kernelClaims)
	beliefClaims := make([]KnowledgeClaimRecord, 0, len(hardConstraints)+len(decisions)+len(blockerSymptoms)+len(blockerHypotheses))
	beliefClaims = append(beliefClaims, hardConstraints...)
	beliefClaims = append(beliefClaims, decisions...)
	beliefClaims = append(beliefClaims, blockerSymptoms...)
	beliefClaims = append(beliefClaims, blockerHypotheses...)
	beliefUpdates := s.buildRSPBeliefUpdatesForClaims(ctx, beliefClaims)

	basisRefs := make([]MemoryPacketBasisRef, 0, 64)
	basisRefs = appendMemoryPacketBasisRef(basisRefs, "task", packetCtx.taskID, "task", packetCtx.hydration.Task.UpdatedAt)
	if packetCtx.session != nil {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "agent_session", packetCtx.session.SessionID, "session", packetCtx.session.CreatedAt)
	}
	for _, record := range packetCtx.hydration.Docs {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "workspace_doc", record.DocKey, "doc", record.SHA)
	}
	for _, record := range packetCtx.hydration.Artifacts {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "workspace_artifact", record.ArtifactRef, "artifact", record.ArtifactID)
	}
	for _, record := range hardConstraints {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "hard_constraint", memoryPacketClaimVersionToken(record))
	}
	for _, record := range decisions {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "accepted_decision", memoryPacketClaimVersionToken(record))
	}
	for _, record := range blockerSymptoms {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "active_blocker", memoryPacketClaimVersionToken(record))
	}
	for _, record := range blockerHypotheses {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "knowledge_claim", record.ClaimID, "blocker_hypothesis", memoryPacketClaimVersionToken(record))
	}
	for _, record := range packetCtx.openQueues {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "operator_queue", record.QueueID, "open_queue", record.UpdatedAt)
	}
	if handoff := selectMemoryPacketLastVerifiedHandoff(packetCtx.episodePacks); handoff != nil {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "episode_pack", handoff.PackID, "verified_handoff", handoff.UpdatedAt)
	}
	for _, item := range cluster.CriticalAmbiguities {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "tension", item.TensionID, "critical_ambiguity", item.LastSeenAt)
	}
	for _, item := range cluster.ActiveContradictions {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "tension", item.TensionID, "active_contradiction", item.LastSeenAt)
	}
	for _, ref := range cluster.RelatedSegmentRefs {
		basisRefs = appendMemoryPacketBasisRef(basisRefs, "segment_ref", ref, "related_segment", ref)
	}
	if packetCtx.locus.SegmentReport != nil {
		for _, segment := range packetCtx.locus.SegmentReport.Segments {
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "segment_ref", segment.SegmentRef, "segment", fmt.Sprintf("%s:%d:%d", segment.SourceRef, segment.StartLine, segment.EndLine))
		}
	}

	packet := MemoryKernelPacket{
		Meta: MemoryPacketMeta{
			Contract:       memoryPacketContract,
			PacketKind:     memoryPacketKindKernel,
			PacketKey:      buildMemoryPacketKey(memoryPacketKindKernel, packetCtx.workspaceID, packetCtx.taskID, packetCtx.sessionID, ""),
			BasisDigest:    buildMemoryPacketBasisDigest(basisRefs),
			SchemaVersion:  memoryPacketSchemaVersion,
			GeneratedAt:    now,
			WorkspaceID:    packetCtx.workspaceID,
			TaskID:         packetCtx.taskID,
			SessionID:      packetCtx.sessionID,
			ProtoClusterID: cluster.ProtoClusterID,
			Scope:          buildMemoryPacketScope(packetCtx.taskID, packetCtx.sessionID, ""),
		},
		Task:          packetCtx.hydration.Task,
		WorkspaceTask: packetCtx.hydration.WorkspaceTask,
		TaskCharter: MemoryPacketTaskCharter{
			Title:               packetCtx.hydration.Task.Title,
			Description:         packetCtx.hydration.Task.Description,
			Priority:            packetCtx.hydration.Task.Priority,
			Status:              packetCtx.hydration.Task.Status,
			TaskKind:            packetCtx.hydration.Task.TaskKind,
			TaskTemplate:        packetCtx.hydration.Task.TaskTemplate,
			TaskClass:           packetCtx.hydration.Task.TaskClass,
			TaskClassSource:     packetCtx.hydration.Task.TaskClassSource,
			TaskClassUpdatedAt:  packetCtx.hydration.Task.TaskClassUpdatedAt,
			ProjectID:           packetCtx.hydration.Task.ProjectID,
			ProjectLane:         packetCtx.hydration.Task.ProjectLane,
			RequiresProjectGate: packetCtx.hydration.Task.RequiresProjectGate,
		},
		Docs:          append([]WorkspaceDocRecord(nil), packetCtx.hydration.Docs...),
		Artifacts:     append([]WorkspaceArtifactRecord(nil), packetCtx.hydration.Artifacts...),
		TaskLinks:     append([]WorkspaceTaskLinkRecord(nil), packetCtx.hydration.TaskLinks...),
		RelatedTasks:  append([]TaskStatus(nil), packetCtx.hydration.RelatedTasks...),
		SegmentReport: packetCtx.locus.SegmentReport,
		Coordination: MemoryPacketKernelCoordination{
			HardConstraints:      hardConstraints,
			AcceptedDecisions:    decisions,
			DecisionRecords:      canonicalizeMemoryPacketDecisionRecords(decisions),
			ActiveBlockers:       blockerSymptoms,
			BlockerSymptoms:      canonicalizeMemoryPacketBlockerSymptoms(blockerSymptoms),
			BlockerHypotheses:    canonicalizeMemoryPacketBlockerHypotheses(blockerHypotheses),
			BeliefUpdates:        beliefUpdates,
			OpenQueues:           append([]OperatorQueueRecord(nil), packetCtx.openQueues...),
			CriticalAmbiguities:  cluster.CriticalAmbiguities,
			ActiveContradictions: cluster.ActiveContradictions,
			LastVerifiedHandoff:  selectMemoryPacketLastVerifiedHandoff(packetCtx.episodePacks),
		},
		Cluster:        cluster,
		ClaimFreshness: cloneKnowledgeClaimFreshnessSummary(packetCtx.claimFreshness),
		BasisRefs:      basisRefs,
	}
	return packet
}

func buildMemoryPacketClusterContext(locus InstrumentationLocusBundle) MemoryPacketClusterContext {
	cluster := MemoryPacketClusterContext{
		Resolved:           locus.Resolved,
		ProtoClusterID:     strings.TrimSpace(locus.ProtoClusterID),
		Frontier:           append([]TensionFrontierItem(nil), locus.Frontier...),
		RelatedSegmentRefs: append([]string(nil), locus.RelatedSegmentRefs...),
	}
	if locus.DominantTension != nil {
		cluster.DominantTension = &TensionFrontierItem{
			TensionID:      locus.DominantTension.Tension.TensionID,
			ProtoClusterID: locus.DominantTension.Tension.ProtoClusterID,
			TensionType:    locus.DominantTension.Tension.TensionType,
			ReviewStatus:   locus.DominantTension.Tension.ReviewStatus,
			Title:          locus.DominantTension.Tension.Title,
			Summary:        locus.DominantTension.Tension.Summary,
			SurfaceScore:   locus.DominantTension.Tension.SurfaceScore,
			BaseScore:      locus.DominantTension.Tension.BaseScore,
			EvidenceCount:  locus.DominantTension.Tension.EvidenceCount,
			LastSeenAt:     locus.DominantTension.Tension.LastSeenAt,
		}
	}
	cluster.CriticalAmbiguities = filterMemoryPacketFrontier(locus.Frontier, "ambiguity")
	cluster.ActiveContradictions = filterMemoryPacketFrontier(locus.Frontier, "contradiction")
	return cluster
}

func filterMemoryPacketFrontier(frontier []TensionFrontierItem, tensionType string) []TensionFrontierItem {
	out := make([]TensionFrontierItem, 0, len(frontier))
	for _, item := range frontier {
		if strings.EqualFold(strings.TrimSpace(item.TensionType), tensionType) {
			out = append(out, item)
		}
	}
	return out
}

func splitMemoryPacketKernelClaims(claims []KnowledgeClaimRecord) ([]KnowledgeClaimRecord, []KnowledgeClaimRecord, []KnowledgeClaimRecord, []KnowledgeClaimRecord) {
	hardConstraints := []KnowledgeClaimRecord{}
	decisions := []KnowledgeClaimRecord{}
	blockerSymptoms := []KnowledgeClaimRecord{}
	blockerHypotheses := []KnowledgeClaimRecord{}
	for _, claim := range claims {
		if !isKnowledgeClaimOperationalStatus(claim.Status) {
			continue
		}
		switch strings.TrimSpace(claim.ClaimType) {
		case "CONSTRAINT":
			hardConstraints = append(hardConstraints, claim)
		case "DECISION", "DECISION_RECORD":
			decisions = append(decisions, claim)
		case "BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM":
			blockerSymptoms = append(blockerSymptoms, claim)
		case "BLOCKER_HYPOTHESIS":
			blockerHypotheses = append(blockerHypotheses, claim)
		}
	}
	return hardConstraints, decisions, blockerSymptoms, blockerHypotheses
}

func canonicalizeMemoryPacketDecisionRecords(claims []KnowledgeClaimRecord) []KnowledgeClaimRecord {
	out := make([]KnowledgeClaimRecord, 0, len(claims))
	for _, claim := range claims {
		switch strings.TrimSpace(claim.ClaimType) {
		case "DECISION", "DECISION_RECORD":
			clone := claim
			clone.ClaimType = "DECISION_RECORD"
			out = append(out, clone)
		}
	}
	return out
}

func canonicalizeMemoryPacketBlockerSymptoms(claims []KnowledgeClaimRecord) []KnowledgeClaimRecord {
	out := make([]KnowledgeClaimRecord, 0, len(claims))
	for _, claim := range claims {
		switch strings.TrimSpace(claim.ClaimType) {
		case "BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM":
			clone := claim
			clone.ClaimType = "BLOCKER_SYMPTOM"
			out = append(out, clone)
		}
	}
	return out
}

func canonicalizeMemoryPacketBlockerHypotheses(claims []KnowledgeClaimRecord) []KnowledgeClaimRecord {
	out := make([]KnowledgeClaimRecord, 0, len(claims))
	for _, claim := range claims {
		if strings.TrimSpace(claim.ClaimType) != "BLOCKER_HYPOTHESIS" {
			continue
		}
		clone := claim
		clone.ClaimType = "BLOCKER_HYPOTHESIS"
		out = append(out, clone)
	}
	return out
}

func isMemoryPacketDifferentialClaim(claim KnowledgeClaimRecord) bool {
	switch strings.TrimSpace(claim.ClaimType) {
	case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH":
		return claim.ArchivedAt == nil || strings.TrimSpace(derefString(claim.ArchivedAt)) == "" || isMemoryPacketRecoverableArchivedContrastiveClaim(claim)
	case "HYPOTHESIS", "LESSON", "INCIDENT":
		return claim.ArchivedAt == nil || strings.TrimSpace(derefString(claim.ArchivedAt)) == ""
	default:
		return false
	}
}

func isMemoryPacketDissentClaim(claim KnowledgeClaimRecord) bool {
	switch strings.TrimSpace(claim.ClaimType) {
	case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT":
		return true
	default:
		return false
	}
}

func isMemoryPacketRecoverableArchivedContrastiveClaim(claim KnowledgeClaimRecord) bool {
	if strings.TrimSpace(derefString(claim.ArchivedAt)) == "" {
		return false
	}
	if strings.TrimSpace(claim.LifecycleReason) != rmpArchivedReasonExpired {
		return false
	}
	switch strings.TrimSpace(claim.ClaimType) {
	case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH":
		return true
	default:
		return false
	}
}

func collectMemoryPacketDifferentialClaims(claims []KnowledgeClaimRecord, limit int) []KnowledgeClaimRecord {
	return collectMemoryPacketDifferentialClaimsWithBudget(claims, limit, memoryPacketDefaultDissentQ)
}

func collectMemoryPacketDifferentialClaimsWithBudget(claims []KnowledgeClaimRecord, limit, dissentQuota int) []KnowledgeClaimRecord {
	if limit <= 0 {
		limit = memoryPacketDefaultClaims
	}
	if dissentQuota < 0 {
		dissentQuota = 0
	}
	if dissentQuota > limit {
		dissentQuota = limit
	}
	activeDissent := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)))
	activeOther := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)))
	recoverableDissent := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)))
	recoverableOther := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)))
	for _, claim := range claims {
		if !isMemoryPacketDifferentialClaim(claim) {
			continue
		}
		if isMemoryPacketRecoverableArchivedContrastiveClaim(claim) {
			if isMemoryPacketDissentClaim(claim) {
				recoverableDissent = append(recoverableDissent, claim)
			} else {
				recoverableOther = append(recoverableOther, claim)
			}
			continue
		}
		if isMemoryPacketDissentClaim(claim) {
			activeDissent = append(activeDissent, claim)
		} else {
			activeOther = append(activeOther, claim)
		}
	}
	out := make([]KnowledgeClaimRecord, 0, minInt(limit, len(activeDissent)+len(activeOther)+len(recoverableDissent)+len(recoverableOther)))
	reservedDissent := minInt(limit, minInt(dissentQuota, len(activeDissent)+len(recoverableDissent)))
	usedActiveDissent := minInt(reservedDissent, len(activeDissent))
	out = append(out, activeDissent[:usedActiveDissent]...)
	usedRecoverableDissent := minInt(reservedDissent-len(out), len(recoverableDissent))
	if usedRecoverableDissent > 0 {
		out = append(out, recoverableDissent[:usedRecoverableDissent]...)
	}
	if len(out) >= limit {
		return out[:limit]
	}
	active := append([]KnowledgeClaimRecord{}, activeOther...)
	if usedActiveDissent < len(activeDissent) {
		active = append(active, activeDissent[usedActiveDissent:]...)
	}
	recoverableArchived := append([]KnowledgeClaimRecord{}, recoverableOther...)
	if usedRecoverableDissent < len(recoverableDissent) {
		recoverableArchived = append(recoverableArchived, recoverableDissent[usedRecoverableDissent:]...)
	}
	remaining := limit - len(out)
	recoverableQuota := 0
	if len(recoverableArchived) > 0 {
		switch {
		case len(active) == 0:
			recoverableQuota = 1
		case remaining > 1:
			recoverableQuota = maxInt(1, remaining/3)
			if recoverableQuota >= remaining {
				recoverableQuota = remaining - 1
			}
		}
		if recoverableQuota > len(recoverableArchived) {
			recoverableQuota = len(recoverableArchived)
		}
	}
	activeBudget := remaining - recoverableQuota
	out = append(out, active[:minInt(activeBudget, len(active))]...)
	out = append(out, recoverableArchived[:recoverableQuota]...)
	if len(out) < limit && len(active) > activeBudget {
		remainder := active[activeBudget:]
		out = append(out, remainder[:minInt(limit-len(out), len(remainder))]...)
	}
	return out
}

func memoryPacketLaneLimit(budget *MemoryPacketBudget, lane string) int {
	defaults := defaultMemoryPacketBudget()
	limit := defaults.Lanes[lane].ItemLimit
	if budget != nil {
		if current, ok := budget.Lanes[lane]; ok && current.ItemLimit > 0 {
			limit = current.ItemLimit
		}
	}
	switch lane {
	case MemoryRetrievalLaneCluster, MemoryRetrievalLaneBridge:
		floor := defaults.CoordinationFloor
		if budget != nil && budget.CoordinationFloor > 0 {
			floor = budget.CoordinationFloor
		}
		if limit < floor {
			limit = floor
		}
	}
	return limit
}

func memoryPacketDissentQuota(budget *MemoryPacketBudget, lane string) int {
	limit := memoryPacketLaneLimit(budget, lane)
	if limit <= 0 {
		return 0
	}
	quota := memoryPacketDefaultDissentQ
	if budget != nil && budget.DissentQuota > 0 {
		quota = budget.DissentQuota
	}
	if quota > limit {
		return limit
	}
	return quota
}

func memoryPacketCoordinationLimit(budget *MemoryPacketBudget) int {
	defaults := defaultMemoryPacketBudget()
	limit := defaults.Lanes[MemoryRetrievalLaneCoordination].ItemLimit
	if budget != nil {
		if current, ok := budget.Lanes[MemoryRetrievalLaneCoordination]; ok && current.ItemLimit > 0 {
			limit = current.ItemLimit
		}
	}
	floor := memoryPacketDefaultCoordFloorValue(budget)
	if limit < floor {
		limit = floor
	}
	return limit
}

func memoryPacketAdaptiveCoordinationLimit(budget *MemoryPacketBudget, locus InstrumentationLocusBundle, openQueues []OperatorQueueRecord, explicitCoordinationLane bool) int {
	limit := memoryPacketCoordinationLimit(budget)
	if explicitCoordinationLane || !memoryPacketHasCoordinationPressure(locus, openQueues) {
		return limit
	}
	return limit + memoryPacketAdaptiveCoordBump
}

func memoryPacketAdaptiveBridgeLimit(budget *MemoryPacketBudget, locus InstrumentationLocusBundle, explicitBridgeLane bool) int {
	limit := memoryPacketLaneLimit(budget, MemoryRetrievalLaneBridge)
	if explicitBridgeLane || !memoryPacketHasBridgePressure(locus) {
		return limit
	}
	return limit + memoryPacketAdaptiveCoordBump
}

func memoryPacketDefaultCoordFloorValue(budget *MemoryPacketBudget) int {
	floor := memoryPacketDefaultCoordFloor
	if budget != nil && budget.CoordinationFloor > 0 {
		floor = budget.CoordinationFloor
	}
	return floor
}

func memoryPacketHasExplicitLane(budget *MemoryPacketBudget, lane string) bool {
	if budget == nil || budget.Lanes == nil {
		return false
	}
	current, ok := budget.Lanes[lane]
	return ok && current.ItemLimit > 0
}

func memoryPacketHasCoordinationPressure(locus InstrumentationLocusBundle, openQueues []OperatorQueueRecord) bool {
	if locus.Control != nil &&
		!locus.Control.Cluster.MetricsMissing &&
		!locus.Control.Cluster.BasisStale &&
		locus.Control.Cluster.Signals.CoordinationPressure >= memoryPacketAdaptiveCoordSignal {
		return true
	}
	if len(openQueues) > 0 {
		return true
	}
	if len(filterMemoryPacketFrontier(locus.Frontier, "ambiguity")) > 0 {
		return true
	}
	return len(filterMemoryPacketFrontier(locus.Frontier, "contradiction")) > 0
}

func memoryPacketHasBridgePressure(locus InstrumentationLocusBundle) bool {
	if locus.Control == nil ||
		locus.Control.Cluster.MetricsMissing ||
		locus.Control.Cluster.BasisStale ||
		locus.Control.Cluster.Signals.CoordinationPressure < memoryPacketAdaptiveCoordSignal {
		return false
	}
	return locus.Control.Cluster.ConfirmedCountsByType["bridge"] > 0
}

func selectMemoryPacketLastVerifiedHandoff(packs []EpisodePackRecord) *EpisodePackRecord {
	var fallback *EpisodePackRecord
	for idx := range packs {
		record := packs[idx]
		switch strings.TrimSpace(record.PackType) {
		case episodePackTypeSessionHandoff, episodePackTypeSessionTakeover:
		default:
			continue
		}
		if record.PackMode == episodePackModeComplete {
			copy := record
			return &copy
		}
		if fallback == nil {
			copy := record
			fallback = &copy
		}
	}
	return fallback
}

func resolveMemoryPacketFocusSummary(packetCtx memoryPacketBuildContext) string {
	if packetCtx.sessionState != nil && strings.TrimSpace(packetCtx.sessionState.Summary) != "" {
		return strings.TrimSpace(packetCtx.sessionState.Summary)
	}
	if packetCtx.hydration.WorkspaceTask != nil && packetCtx.hydration.WorkspaceTask.ClaimSummary != nil {
		return strings.TrimSpace(*packetCtx.hydration.WorkspaceTask.ClaimSummary)
	}
	return ""
}

func (s *Store) buildRSPStateEstimateForPacket(packetCtx memoryPacketBuildContext) *RSPStateReport {
	if strings.TrimSpace(packetCtx.agentID) == "" {
		return nil
	}
	report := buildRSPStateReportFromBundle(s, context.Background(), RSPStateReportFilter{
		WorkspaceID:    packetCtx.workspaceID,
		ProtoClusterID: packetCtx.locus.ProtoClusterID,
		AgentID:        packetCtx.agentID,
		TaskID:         packetCtx.taskID,
		SessionID:      packetCtx.sessionID,
		DocKeys:        append([]string(nil), packetCtx.docKeys...),
		ArtifactRefs:   append([]string(nil), packetCtx.artifactRefs...),
	}, packetCtx.locus)
	return &report
}

func appendMemoryPacketRSPStateBasisRefs(basisRefs []MemoryPacketBasisRef, locus InstrumentationLocusBundle) []MemoryPacketBasisRef {
	if locus.ControlState != nil {
		if eventID := strings.TrimSpace(locus.ControlState.State.State.LastTickEventID); eventID != "" {
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "runtime_event", eventID, "rsp_state_control_tick", locus.ControlState.State.State.LastTickAt)
		}
	}
	if locus.MemoryCoherence != nil {
		scopeID := strings.TrimSpace(locus.MemoryCoherence.AgentID)
		if sessionID := strings.TrimSpace(locus.MemoryCoherence.SessionID); sessionID != "" {
			scopeID += "/" + sessionID
		}
		if scopeID != "" {
			token := latestNonEmptyTimestamp(locus.MemoryCoherence.InvalidationUpdatedAt, locus.MemoryCoherence.MetricsUpdatedAt, locus.MemoryCoherence.ResidencyUpdatedAt)
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "memory_coherence_scope", scopeID, "rsp_state_memory_coherence", token)
		}
		if reportID := strings.TrimSpace(locus.MemoryCoherence.MetricsReportID); reportID != "" {
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "memory_metrics_report", reportID, "rsp_state_memory_metrics", locus.MemoryCoherence.MetricsUpdatedAt)
		}
		if reportID := strings.TrimSpace(locus.MemoryCoherence.ResidencyReportID); reportID != "" {
			basisRefs = appendMemoryPacketBasisRef(basisRefs, "memory_residency_report", reportID, "rsp_state_memory_residency", locus.MemoryCoherence.ResidencyUpdatedAt)
		}
	}
	return basisRefs
}

func buildMemoryPacketShellLoopSets(packs []EpisodePackRecord) ([]string, []string) {
	openLoops := make([]string, 0, memoryPacketDefaultShellLoops)
	repairChains := make([]string, 0, memoryPacketDefaultShellLoops)
	for _, record := range packs {
		for _, loop := range record.OpenLoops {
			loop = strings.TrimSpace(loop)
			if loop == "" {
				continue
			}
			openLoops = appendUniqueString(openLoops, loop)
			if len(openLoops) >= memoryPacketDefaultShellLoops {
				break
			}
		}
		for _, repair := range record.FailureRepairChain {
			repair = strings.TrimSpace(repair)
			if repair == "" {
				continue
			}
			repairChains = appendUniqueString(repairChains, repair)
			if len(repairChains) >= memoryPacketDefaultShellLoops {
				break
			}
		}
	}
	return openLoops, repairChains
}

func appendUniqueString(values []string, candidate string) []string {
	for _, value := range values {
		if value == candidate {
			return values
		}
	}
	return append(values, candidate)
}

func appendMemoryPacketBasisRef(refs []MemoryPacketBasisRef, refKind, refID, role, versionToken string) []MemoryPacketBasisRef {
	refKind = strings.TrimSpace(refKind)
	refID = strings.TrimSpace(refID)
	if refKind == "" || refID == "" {
		return refs
	}
	role = strings.TrimSpace(role)
	versionToken = strings.TrimSpace(versionToken)
	for _, ref := range refs {
		if ref.RefKind == refKind && ref.RefID == refID && ref.Role == role && ref.VersionToken == versionToken {
			return refs
		}
	}
	return append(refs, MemoryPacketBasisRef{
		RefKind:      refKind,
		RefID:        refID,
		Role:         role,
		VersionToken: versionToken,
	})
}

func buildMemoryPacketKey(packetKind, workspaceID, taskID, sessionID, agentID string) string {
	payload := strings.Join([]string{
		strings.TrimSpace(strings.ToUpper(packetKind)),
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(taskID),
		strings.TrimSpace(sessionID),
		strings.TrimSpace(agentID),
	}, "|")
	sum := sha256.Sum256([]byte(payload))
	return "memory_packet:" + strings.ToLower(strings.TrimSpace(packetKind)) + ":" + hex.EncodeToString(sum[:12])
}

func buildMemoryPacketBasisDigest(refs []MemoryPacketBasisRef) string {
	normalized := append([]MemoryPacketBasisRef(nil), refs...)
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].RefKind != normalized[j].RefKind {
			return normalized[i].RefKind < normalized[j].RefKind
		}
		if normalized[i].RefID != normalized[j].RefID {
			return normalized[i].RefID < normalized[j].RefID
		}
		if normalized[i].Role != normalized[j].Role {
			return normalized[i].Role < normalized[j].Role
		}
		return normalized[i].VersionToken < normalized[j].VersionToken
	})
	hash := sha256.New()
	for _, ref := range normalized {
		hash.Write([]byte(ref.RefKind))
		hash.Write([]byte{0})
		hash.Write([]byte(ref.RefID))
		hash.Write([]byte{0})
		hash.Write([]byte(ref.Role))
		hash.Write([]byte{0})
		hash.Write([]byte(ref.VersionToken))
		hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func buildMemoryPacketScope(taskID, sessionID, agentID string) string {
	scope := []string{}
	if strings.TrimSpace(taskID) != "" {
		scope = append(scope, "task")
	}
	if strings.TrimSpace(sessionID) != "" {
		scope = append(scope, "session")
	}
	if strings.TrimSpace(agentID) != "" {
		scope = append(scope, "agent")
	}
	if len(scope) == 0 {
		return "workspace"
	}
	return strings.Join(scope, "/")
}

func memoryPacketClaimVersionToken(claim KnowledgeClaimRecord) string {
	return strings.Join([]string{
		strings.TrimSpace(claim.Status),
		strings.TrimSpace(claim.UpdatedAt),
		strings.TrimSpace(claim.SupersededByClaimID),
		strings.TrimSpace(claim.ConflictsClaimID),
		strings.TrimSpace(derefString(claim.ReviewDueAt)),
		strings.TrimSpace(derefString(claim.ReviewedAt)),
		strings.TrimSpace(derefString(claim.ReviewedBy)),
		strings.TrimSpace(claim.LifecycleReason),
		strings.TrimSpace(claim.Freshness),
		strings.TrimSpace(claim.ProvenanceStrength),
		strings.TrimSpace(claim.DowngradeReason),
	}, "|")
}

func cloneKnowledgeClaimFreshnessSummary(summary *KnowledgeClaimFreshnessSummary) *KnowledgeClaimFreshnessSummary {
	if summary == nil {
		return nil
	}
	clone := *summary
	clone.AttentionReasons = append([]string(nil), summary.AttentionReasons...)
	clone.Examples = append([]KnowledgeClaimFreshnessExample(nil), summary.Examples...)
	return &clone
}

func collectMemoryPacketDocKeys(docs []WorkspaceDocRecord, preferred []string) []string {
	keys := append([]string{}, preferred...)
	for _, record := range docs {
		if trimmed := strings.TrimSpace(record.DocKey); trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	return uniqueTrimmedLocusStrings(keys)
}

func (s *Store) listMemoryPacketSelectedDocs(ctx context.Context, workspaceID string, docKeys []string) ([]WorkspaceDocRecord, error) {
	keys := uniqueTrimmedLocusStrings(docKeys)
	if len(keys) == 0 {
		return []WorkspaceDocRecord{}, nil
	}
	out := make([]WorkspaceDocRecord, 0, len(keys))
	for _, docKey := range keys {
		record, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "not found") {
				continue
			}
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func (s *Store) listMemoryPacketClaims(ctx context.Context, workspaceID, taskID, sessionID, referenceAt string, semanticLimit, contrastiveLimit, dissentQuota int) ([]KnowledgeClaimRecord, error) {
	semanticClaims, err := s.listMemoryPacketActiveClaimTypes(ctx, workspaceID, taskID, sessionID, referenceAt, semanticLimit,
		"FACT", "ENTITY", "UPDATE_DIGEST", "SUMMARY", "EXPERIENCE",
	)
	if err != nil {
		return nil, err
	}
	activeDissentLimit := maxInt(contrastiveLimit, dissentQuota)
	activeDissentClaims, err := s.listMemoryPacketActiveClaimTypes(ctx, workspaceID, taskID, sessionID, referenceAt, activeDissentLimit,
		"DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT",
	)
	if err != nil {
		return nil, err
	}
	activeOtherContrastiveClaims, err := s.listMemoryPacketActiveClaimTypes(ctx, workspaceID, taskID, sessionID, referenceAt, contrastiveLimit,
		"ALTERNATIVE_BRANCH", "HYPOTHESIS", "LESSON", "INCIDENT",
	)
	if err != nil {
		return nil, err
	}
	if contrastiveLimit > 0 && len(activeOtherContrastiveClaims) > contrastiveLimit {
		activeOtherContrastiveClaims = activeOtherContrastiveClaims[:contrastiveLimit]
	}
	recoverableClaims, err := s.listMemoryPacketRecoverableArchivedContrastiveClaims(ctx, workspaceID, taskID, sessionID, referenceAt, contrastiveLimit)
	if err != nil {
		return nil, err
	}
	mergedLimit := semanticLimit + activeDissentLimit + contrastiveLimit + contrastiveLimit
	if mergedLimit <= 0 {
		mergedLimit = maxInt(memoryPacketDefaultClaims, semanticLimit)
	}
	merged := make([]KnowledgeClaimRecord, 0, minInt(mergedLimit, len(semanticClaims)+len(activeDissentClaims)+len(activeOtherContrastiveClaims)+len(recoverableClaims)))
	seen := map[string]struct{}{}
	for _, record := range append(append(append(semanticClaims, activeDissentClaims...), activeOtherContrastiveClaims...), recoverableClaims...) {
		if _, ok := seen[record.ClaimID]; ok {
			continue
		}
		seen[record.ClaimID] = struct{}{}
		merged = append(merged, record)
		if len(merged) >= mergedLimit {
			break
		}
	}
	return merged, nil
}

func (s *Store) listMemoryPacketProceduralClaims(ctx context.Context, workspaceID, taskID, sessionID, referenceAt string, limit int) ([]KnowledgeClaimRecord, error) {
	if limit <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	claims, err := s.listMemoryPacketActiveClaimTypes(ctx, workspaceID, taskID, sessionID, referenceAt, limit, "PROCEDURE", "ANTI_PROCEDURE")
	if err != nil {
		return nil, err
	}
	sortMemoryPacketClaims(claims)
	if len(claims) > limit {
		claims = claims[:limit]
	}
	return claims, nil
}

func (s *Store) listMemoryPacketIdentityMemories(ctx context.Context, workspaceID, taskID, sessionID, agentID string, limit int) ([]WorkspaceMemoryRecord, error) {
	if limit <= 0 {
		return []WorkspaceMemoryRecord{}, nil
	}
	fetchLimit := maxInt(limit*2, limit)
	seenMemoryIDs := map[string]struct{}{}
	taskStage, err := s.listMemoryPacketIdentityStage(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		Limit:       fetchLimit,
	}, seenMemoryIDs)
	if err != nil {
		return nil, err
	}
	sessionStage, err := s.listMemoryPacketIdentityStage(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		Limit:       fetchLimit,
	}, seenMemoryIDs)
	if err != nil {
		return nil, err
	}
	workspaceStage, err := s.listMemoryPacketIdentityStage(ctx, WorkspaceMemoryFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       fetchLimit,
	}, seenMemoryIDs)
	if err != nil {
		return nil, err
	}

	candidates := append(append(append([]WorkspaceMemoryRecord{}, taskStage...), sessionStage...), workspaceStage...)
	if len(candidates) == 0 {
		return []WorkspaceMemoryRecord{}, nil
	}
	sortMemoryPacketIdentityMemories(candidates)
	lineageKeys, err := s.loadMemoryPacketIdentityLineageKeys(ctx, workspaceID, candidates)
	if err != nil {
		return nil, err
	}
	out := make([]WorkspaceMemoryRecord, 0, minInt(limit, len(candidates)))
	seenLineages := map[string]struct{}{}
	appendStage := func(stage []WorkspaceMemoryRecord) {
		if len(out) >= limit || len(stage) == 0 {
			return
		}
		sortMemoryPacketIdentityMemories(stage)
		for _, record := range stage {
			key := memoryPacketIdentityLineageKey(record, lineageKeys)
			if _, ok := seenLineages[key]; ok {
				continue
			}
			seenLineages[key] = struct{}{}
			out = append(out, record)
			if len(out) >= limit {
				return
			}
		}
	}
	appendStage(taskStage)
	appendStage(sessionStage)
	appendStage(workspaceStage)
	return out, nil
}

func (s *Store) listMemoryPacketIdentityStage(ctx context.Context, filter WorkspaceMemoryFilter, seenMemoryIDs map[string]struct{}) ([]WorkspaceMemoryRecord, error) {
	if filter.Limit <= 0 {
		return []WorkspaceMemoryRecord{}, nil
	}
	stage := make([]WorkspaceMemoryRecord, 0, filter.Limit)
	for _, memoryType := range []string{"SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE"} {
		queryFilter := filter
		queryFilter.MemoryType = memoryType
		records, err := s.ListWorkspaceMemory(ctx, queryFilter)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
				continue
			}
			memoryID := strings.TrimSpace(record.MemoryID)
			if memoryID == "" {
				continue
			}
			if _, ok := seenMemoryIDs[memoryID]; ok {
				continue
			}
			seenMemoryIDs[memoryID] = struct{}{}
			stage = append(stage, record)
		}
	}
	return stage, nil
}

func (s *Store) loadMemoryPacketIdentityLineageKeys(ctx context.Context, workspaceID string, records []WorkspaceMemoryRecord) (map[string]string, error) {
	keys := make([]string, 0, len(records))
	seen := map[string]struct{}{}
	for _, record := range records {
		memoryID := strings.TrimSpace(record.MemoryID)
		if memoryID == "" {
			continue
		}
		if _, ok := seen[memoryID]; ok {
			continue
		}
		seen[memoryID] = struct{}{}
		keys = append(keys, memoryID)
	}
	if len(keys) == 0 {
		return map[string]string{}, nil
	}
	args := make([]any, 0, len(keys)+2)
	args = append(args, strings.TrimSpace(workspaceID), "workspace_memory")
	for _, memoryID := range keys {
		args = append(args, memoryID)
	}
	query := `SELECT origin_id, semantic_lineage_id
		FROM memory_nodes
		WHERE workspace_id = ? AND origin_kind = ? AND origin_id IN (` + placeholders(len(keys)) + `)`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lineages := make(map[string]string, len(keys))
	for rows.Next() {
		var originID, lineage string
		if err := rows.Scan(&originID, &lineage); err != nil {
			return nil, err
		}
		lineages[strings.TrimSpace(originID)] = strings.TrimSpace(lineage)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return lineages, nil
}

func memoryPacketIdentityLineageKey(record WorkspaceMemoryRecord, lineages map[string]string) string {
	memoryID := strings.TrimSpace(record.MemoryID)
	if lineage := strings.TrimSpace(lineages[memoryID]); lineage != "" {
		return lineage
	}
	return memoryID
}

func memoryPacketIdentityBasisRole(record WorkspaceMemoryRecord) string {
	memoryType := strings.ToLower(strings.TrimSpace(record.MemoryType))
	if memoryType == "" {
		memoryType = "memory"
	}
	scope := "workspace"
	switch {
	case strings.TrimSpace(record.TaskID) != "":
		scope = "task"
	case strings.TrimSpace(record.SessionID) != "":
		scope = "session"
	case strings.TrimSpace(record.AgentID) != "":
		scope = "workspace_agent"
	}
	return strings.Join([]string{"identity", memoryType, scope}, "_")
}

func (s *Store) listMemoryPacketActiveClaimTypes(ctx context.Context, workspaceID, taskID, sessionID, referenceAt string, limit int, claimTypes ...string) ([]KnowledgeClaimRecord, error) {
	if limit <= 0 || len(claimTypes) == 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	combined := make([]KnowledgeClaimRecord, 0, limit*len(claimTypes))
	seen := map[string]struct{}{}
	for _, claimType := range claimTypes {
		claims, err := s.listMemoryPacketAuthoritativeClaimScopes(ctx, KnowledgeClaimFilter{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			ClaimType:   claimType,
			Limit:       limit,
		}, sessionID, referenceAt)
		if err != nil {
			return nil, err
		}
		for _, record := range claims {
			if strings.TrimSpace(derefString(record.ArchivedAt)) != "" {
				continue
			}
			if !isKnowledgeClaimAuthoritativeForMemoryPacket(record, referenceAt) {
				continue
			}
			if _, ok := seen[record.ClaimID]; ok {
				continue
			}
			seen[record.ClaimID] = struct{}{}
			combined = append(combined, decorateKnowledgeClaimForMemoryPacket(record, referenceAt, "authoritative"))
		}
	}
	sortMemoryPacketClaims(combined)
	if len(combined) > limit {
		return combined[:limit], nil
	}
	return combined, nil
}

func (s *Store) listMemoryPacketAuthoritativeClaimScopes(ctx context.Context, filter KnowledgeClaimFilter, sessionID, referenceAt string) ([]KnowledgeClaimRecord, error) {
	limit := filter.Limit
	if limit <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	claims, err := s.listMemoryPacketAuthoritativeClaimQuery(ctx, filter, referenceAt, limit)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return claims, nil
	}
	sessionFilter := filter
	sessionFilter.TaskID = ""
	sessionFilter.SessionID = sessionID
	sessionClaims, err := s.listMemoryPacketAuthoritativeClaimQuery(ctx, sessionFilter, referenceAt, limit)
	if err != nil {
		return nil, err
	}
	merged := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)+len(sessionClaims)))
	seen := map[string]struct{}{}
	for _, record := range append(claims, sessionClaims...) {
		if _, ok := seen[record.ClaimID]; ok {
			continue
		}
		seen[record.ClaimID] = struct{}{}
		merged = append(merged, record)
		if len(merged) >= limit {
			break
		}
	}
	return merged, nil
}

func (s *Store) listMemoryPacketAuthoritativeClaimQuery(ctx context.Context, filter KnowledgeClaimFilter, referenceAt string, limit int) ([]KnowledgeClaimRecord, error) {
	if limit <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	filter.IncludeArchived = false
	args := make([]any, 0, 12)
	where := buildKnowledgeClaimWhere(filter, "", &args)
	where = append(where,
		`status IN ('ACTIVE','CONFIRMED')`,
		`COALESCE(source_kind,'') <> ''`,
		`COALESCE(source_id,'') <> ''`,
		`COALESCE(superseded_by_claim_id,'') = ''`,
		`COALESCE(conflicts_claim_id,'') = ''`,
	)
	if referenceAt = strings.TrimSpace(referenceAt); referenceAt != "" {
		where = append(where, `(review_due_at IS NULL OR review_due_at = '' OR review_due_at > ?)`)
		args = append(args, referenceAt)
	}
	query := strings.Builder{}
	query.WriteString(`SELECT ` + knowledgeClaimSelectColumns("") + ` FROM knowledge_claims`)
	if len(where) > 0 {
		query.WriteString(` WHERE `)
		query.WriteString(strings.Join(where, " AND "))
	}
	query.WriteString(` ORDER BY ` + knowledgeClaimStatusPrioritySQL("") + ` DESC, ` + knowledgeClaimTypePrioritySQL("") + ` DESC, updated_at DESC, confidence DESC, claim_id DESC LIMIT ?`)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list memory packet authoritative knowledge claims: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func (s *Store) listMemoryPacketClaimScopes(ctx context.Context, filter KnowledgeClaimFilter, sessionID string) ([]KnowledgeClaimRecord, error) {
	limit := filter.Limit
	if limit <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	claims, err := s.ListKnowledgeClaims(ctx, filter)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return claims, nil
	}
	sessionFilter := filter
	sessionFilter.TaskID = ""
	sessionFilter.SessionID = sessionID
	sessionClaims, err := s.ListKnowledgeClaims(ctx, sessionFilter)
	if err != nil {
		return nil, err
	}
	merged := make([]KnowledgeClaimRecord, 0, minInt(limit, len(claims)+len(sessionClaims)))
	seen := map[string]struct{}{}
	for _, record := range append(claims, sessionClaims...) {
		if _, ok := seen[record.ClaimID]; ok {
			continue
		}
		seen[record.ClaimID] = struct{}{}
		merged = append(merged, record)
		if len(merged) >= limit {
			break
		}
	}
	return merged, nil
}

func (s *Store) listMemoryPacketRecoverableArchivedContrastiveClaims(ctx context.Context, workspaceID, taskID, sessionID, referenceAt string, limit int) ([]KnowledgeClaimRecord, error) {
	if limit <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	combined := make([]KnowledgeClaimRecord, 0, limit*2)
	seen := map[string]struct{}{}
	for _, claimType := range []string{"DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH"} {
		claims, err := s.listMemoryPacketClaimScopes(ctx, KnowledgeClaimFilter{
			WorkspaceID:     workspaceID,
			TaskID:          taskID,
			ClaimType:       claimType,
			Status:          "ARCHIVED",
			IncludeArchived: true,
			Limit:           limit,
		}, sessionID)
		if err != nil {
			return nil, err
		}
		for _, record := range claims {
			if !isMemoryPacketRecoverableArchivedContrastiveClaim(record) {
				continue
			}
			if _, ok := seen[record.ClaimID]; ok {
				continue
			}
			seen[record.ClaimID] = struct{}{}
			combined = append(combined, decorateKnowledgeClaimForMemoryPacket(record, referenceAt, "recoverable_archived_contrastive"))
		}
	}
	sortMemoryPacketClaims(combined)
	if len(combined) > limit {
		return combined[:limit], nil
	}
	return combined, nil
}

func sortMemoryPacketClaims(claims []KnowledgeClaimRecord) {
	sort.SliceStable(claims, func(i, j int) bool {
		if claims[i].UpdatedAt != claims[j].UpdatedAt {
			return claims[i].UpdatedAt > claims[j].UpdatedAt
		}
		if claims[i].Confidence != claims[j].Confidence {
			return claims[i].Confidence > claims[j].Confidence
		}
		return claims[i].ClaimID > claims[j].ClaimID
	})
}

func sortMemoryPacketIdentityMemories(records []WorkspaceMemoryRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].UpdatedAt != records[j].UpdatedAt {
			return records[i].UpdatedAt > records[j].UpdatedAt
		}
		if records[i].Importance != records[j].Importance {
			return records[i].Importance > records[j].Importance
		}
		return records[i].MemoryID > records[j].MemoryID
	})
}

func mergeMemoryPacketClaims(groups ...[]KnowledgeClaimRecord) []KnowledgeClaimRecord {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	merged := make([]KnowledgeClaimRecord, 0, total)
	seen := map[string]struct{}{}
	for _, group := range groups {
		for _, claim := range group {
			if _, ok := seen[claim.ClaimID]; ok {
				continue
			}
			seen[claim.ClaimID] = struct{}{}
			merged = append(merged, claim)
		}
	}
	sortMemoryPacketClaims(merged)
	return merged
}

func (s *Store) listMemoryPacketKernelCoordinationClaims(ctx context.Context, workspaceID, taskID, sessionID, referenceAt string, floor int) ([]KnowledgeClaimRecord, error) {
	if floor <= 0 {
		return []KnowledgeClaimRecord{}, nil
	}
	combined := make([]KnowledgeClaimRecord, 0, floor*3)
	seen := map[string]struct{}{}
	for _, claimType := range []string{"CONSTRAINT", "DECISION", "DECISION_RECORD", "BLOCKER", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS"} {
		claims, err := s.listMemoryPacketAuthoritativeClaimScopes(ctx, KnowledgeClaimFilter{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			ClaimType:   claimType,
			Limit:       floor,
		}, sessionID, referenceAt)
		if err != nil {
			return nil, err
		}
		for _, record := range claims {
			if !isKnowledgeClaimAuthoritativeForMemoryPacket(record, referenceAt) {
				continue
			}
			if _, ok := seen[record.ClaimID]; ok {
				continue
			}
			seen[record.ClaimID] = struct{}{}
			combined = append(combined, decorateKnowledgeClaimForMemoryPacket(record, referenceAt, "coordination"))
		}
	}
	sortMemoryPacketClaims(combined)
	return combined, nil
}
