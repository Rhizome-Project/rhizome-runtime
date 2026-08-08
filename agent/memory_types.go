package main

import "strings"

type localMemoryNodeType string

const (
	localMemoryNodeRawEvent      localMemoryNodeType = "RAW_EVENT"
	localMemoryNodeRawMessage    localMemoryNodeType = "RAW_MESSAGE"
	localMemoryNodeEpisodePack   localMemoryNodeType = "EPISODE_PACK"
	localMemoryNodeDecision      localMemoryNodeType = "DECISION"
	localMemoryNodeDissent       localMemoryNodeType = "DISSENT"
	localMemoryNodeConstraint    localMemoryNodeType = "CONSTRAINT"
	localMemoryNodeBlocker       localMemoryNodeType = "BLOCKER"
	localMemoryNodeArtifactDelta localMemoryNodeType = "ARTIFACT_DELTA"
	localMemoryNodeHandoff       localMemoryNodeType = "HANDOFF"
	localMemoryNodeProcedure     localMemoryNodeType = "PROCEDURE"
	localMemoryNodeAntiProcedure localMemoryNodeType = "ANTI_PROCEDURE"
	localMemoryNodeClusterBrief  localMemoryNodeType = "CLUSTER_BRIEF"
)

const (
	localMemoryClaimModalityObserved    = "observed"
	localMemoryClaimModalityInferred    = "inferred"
	localMemoryClaimModalityProposed    = "proposed"
	localMemoryClaimModalityDecided     = "decided"
	localMemoryClaimModalityConstrained = "constrained"

	localMemoryWriteStateDraft      = "draft"
	localMemoryWriteStateCandidate  = "candidate"
	localMemoryWriteStatePromoted   = "promoted"
	localMemoryWriteStateSkipped    = "skipped"
	localMemoryWriteStateRejected   = "rejected"
	localMemoryWriteStateSuperseded = "superseded"
)

func normalizeLocalMemoryClaimModality(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case localMemoryClaimModalityObserved:
		return localMemoryClaimModalityObserved
	case localMemoryClaimModalityInferred:
		return localMemoryClaimModalityInferred
	case localMemoryClaimModalityProposed:
		return localMemoryClaimModalityProposed
	case localMemoryClaimModalityDecided:
		return localMemoryClaimModalityDecided
	case localMemoryClaimModalityConstrained:
		return localMemoryClaimModalityConstrained
	case "decision":
		return localMemoryClaimModalityDecided
	case "infer":
		return localMemoryClaimModalityInferred
	case "proposal", "candidate":
		return localMemoryClaimModalityProposed
	case "constraint":
		return localMemoryClaimModalityConstrained
	case "observe":
		return localMemoryClaimModalityObserved
	default:
		return ""
	}
}

func localMemoryClaimModalityForNodeType(nodeType localMemoryNodeType) string {
	switch nodeType {
	case localMemoryNodeDecision:
		return localMemoryClaimModalityDecided
	case localMemoryNodeProcedure, localMemoryNodeAntiProcedure:
		return localMemoryClaimModalityInferred
	case localMemoryNodeConstraint:
		return localMemoryClaimModalityConstrained
	case localMemoryNodeRawEvent, localMemoryNodeRawMessage, localMemoryNodeEpisodePack, localMemoryNodeBlocker, localMemoryNodeDissent, localMemoryNodeHandoff, localMemoryNodeArtifactDelta, localMemoryNodeClusterBrief:
		return localMemoryClaimModalityObserved
	default:
		return localMemoryClaimModalityProposed
	}
}

func normalizeLocalMemoryWriteState(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case localMemoryWriteStateDraft:
		return localMemoryWriteStateDraft
	case localMemoryWriteStateCandidate:
		return localMemoryWriteStateCandidate
	case localMemoryWriteStatePromoted:
		return localMemoryWriteStatePromoted
	case localMemoryWriteStateSkipped:
		return localMemoryWriteStateSkipped
	case localMemoryWriteStateRejected:
		return localMemoryWriteStateRejected
	case localMemoryWriteStateSuperseded:
		return localMemoryWriteStateSuperseded
	case "pending":
		return localMemoryWriteStateCandidate
	case "failed", "error":
		return localMemoryWriteStateRejected
	case "promote":
		return localMemoryWriteStatePromoted
	case "skip":
		return localMemoryWriteStateSkipped
	case "supersede":
		return localMemoryWriteStateSuperseded
	default:
		return ""
	}
}

func localMemoryWriteStateForPersistence(value string) string {
	state := normalizeLocalMemoryWriteState(value)
	if state == "" {
		return localMemoryWriteStateCandidate
	}
	return state
}

type LocalMemoryEvent struct {
	Sequence       int64               `json:"sequence"`
	OccurredAt     string              `json:"occurred_at"`
	NodeType       localMemoryNodeType `json:"node_type"`
	EventKind      string              `json:"event_kind"`
	Summary        string              `json:"summary,omitempty"`
	Details        string              `json:"details,omitempty"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	TensionID      string              `json:"tension_id,omitempty"`
	ProtoClusterID string              `json:"proto_cluster_id,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	SegmentRefs    []string            `json:"segment_refs,omitempty"`
	ConstraintRefs []string            `json:"constraint_refs,omitempty"`
	BlockerKinds   []string            `json:"blocker_kinds,omitempty"`
	Outcome        string              `json:"outcome,omitempty"`
	SourceID       string              `json:"source_id,omitempty"`
	RequiresHuman  bool                `json:"requires_human,omitempty"`
	MetadataJSON   string              `json:"metadata_json,omitempty"`
}

type LocalEpisodeDigest struct {
	ScopeKey         string   `json:"scope_key"`
	ScopeKind        string   `json:"scope_kind"`
	ClaimModality    string   `json:"claim_modality,omitempty"`
	WriteState       string   `json:"write_state,omitempty"`
	AnchorsJSON      string   `json:"anchors_json,omitempty"`
	UpdatedAt        string   `json:"updated_at"`
	EventCount       int      `json:"event_count"`
	MessageCount     int      `json:"message_count"`
	LastSummary      string   `json:"last_summary,omitempty"`
	LastOutcome      string   `json:"last_outcome,omitempty"`
	RecentSummaries  []string `json:"recent_summaries,omitempty"`
	ArtifactRefs     []string `json:"artifact_refs,omitempty"`
	DocKeys          []string `json:"doc_keys,omitempty"`
	BlockerKinds     []string `json:"blocker_kinds,omitempty"`
	OpenLoops        []string `json:"open_loops,omitempty"`
	DissentSummaries []string `json:"dissent_summaries,omitempty"`
	HandoffSummary   string   `json:"handoff_summary,omitempty"`
	ConstraintRefs   []string `json:"constraint_refs,omitempty"`
	ProtoClusterID   string   `json:"proto_cluster_id,omitempty"`
	LatestTensionID  string   `json:"latest_tension_id,omitempty"`
	LatestSessionID  string   `json:"latest_session_id,omitempty"`
}

type LocalPromotionCandidate struct {
	CandidateID    string              `json:"candidate_id"`
	CreatedAt      string              `json:"created_at"`
	NodeType       localMemoryNodeType `json:"node_type"`
	ClaimModality  string              `json:"claim_modality,omitempty"`
	WriteState     string              `json:"write_state,omitempty"`
	AnchorsJSON    string              `json:"anchors_json,omitempty"`
	MemoryType     string              `json:"memory_type,omitempty"`
	SourceID       string              `json:"source_id,omitempty"`
	Title          string              `json:"title,omitempty"`
	Body           string              `json:"body,omitempty"`
	Summary        string              `json:"summary,omitempty"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	TensionID      string              `json:"tension_id,omitempty"`
	ProtoClusterID string              `json:"proto_cluster_id,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	ConstraintRefs []string            `json:"constraint_refs,omitempty"`
	MemoryID       string              `json:"memory_id,omitempty"`
	ClaimID        string              `json:"claim_id,omitempty"`
	AttemptCount   int                 `json:"attempt_count,omitempty"`
	LastAttemptAt  string              `json:"last_attempt_at,omitempty"`
	PromotedAt     string              `json:"promoted_at,omitempty"`
	LastError      string              `json:"last_error,omitempty"`
}

type LocalProcedureMemory struct {
	HintID         string              `json:"hint_id"`
	NodeType       localMemoryNodeType `json:"node_type"`
	Signature      string              `json:"signature"`
	Summary        string              `json:"summary"`
	Guidance       string              `json:"guidance,omitempty"`
	EvidenceCount  int                 `json:"evidence_count"`
	LastSeenAt     string              `json:"last_seen_at,omitempty"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	TensionID      string              `json:"tension_id,omitempty"`
	ProtoClusterID string              `json:"proto_cluster_id,omitempty"`
	ArtifactRefs   []string            `json:"artifact_refs,omitempty"`
	DocKeys        []string            `json:"doc_keys,omitempty"`
	BlockerKinds   []string            `json:"blocker_kinds,omitempty"`
	SourceEventIDs []string            `json:"source_event_ids,omitempty"`
	// DistinctAnchors counts how many distinct task/session anchors contributed
	// evidence to this rule. Used by the constitution promotion guard (Layer A):
	// a rule must generalize across >=2 anchors before it can become global.
	DistinctAnchors int `json:"distinct_anchors,omitempty"`
	// Global marks a derived rule that has been promoted to the always-on
	// constitution tier (explicit + DistinctAnchors>=2 + EvidenceCount>=min).
	Global bool `json:"global,omitempty"`
}

// ConstitutionRule is the unified, anchor-independent view surfaced into every
// memory packet (Layer A). It merges operator-authored seed rules with derived
// rules flagged Global.
type ConstitutionRule struct {
	ID            string
	Text          string
	Kind          string // procedure | anti_procedure | invariant
	Seed          bool
	Priority      int
	EvidenceCount int
}

type LocalMemoryStats struct {
	TotalEvents            int    `json:"total_events"`
	RawMessages            int    `json:"raw_messages"`
	RawEvents              int    `json:"raw_events"`
	EpisodeDigests         int    `json:"episode_digests"`
	Procedures             int    `json:"procedures"`
	AntiProcedures         int    `json:"anti_procedures"`
	PromotionQueue         int    `json:"promotion_queue"`
	P1Hits                 int    `json:"p1_hits"`
	P1Misses               int    `json:"p1_misses"`
	P2Hits                 int    `json:"p2_hits"`
	P2Misses               int    `json:"p2_misses"`
	StaleHits              int    `json:"stale_hits"`
	ConsecutiveP2Misses    int    `json:"consecutive_p2_misses"`
	ConsecutiveStaleReads  int    `json:"consecutive_stale_reads"`
	PacketBuilds           int    `json:"packet_builds"`
	PromotionAttempts      int    `json:"promotion_attempts"`
	PromotionFailures      int    `json:"promotion_failures"`
	LastIngestedAt         string `json:"last_ingested_at,omitempty"`
	LastP2HitAt            string `json:"last_p2_hit_at,omitempty"`
	LastP2MissAt           string `json:"last_p2_miss_at,omitempty"`
	LastStaleHitAt         string `json:"last_stale_hit_at,omitempty"`
	LastPacketBuiltAt      string `json:"last_packet_built_at,omitempty"`
	LastPromotionQueuedAt  string `json:"last_promotion_queued_at,omitempty"`
	LastPromotionAttemptAt string `json:"last_promotion_attempt_at,omitempty"`
	LastPromotionSyncedAt  string `json:"last_promotion_synced_at,omitempty"`
	LastShadowSyncAt       string `json:"last_shadow_sync_at,omitempty"`
}

type LocalMemoryPromotionSummary struct {
	CandidateID    string              `json:"candidate_id"`
	NodeType       localMemoryNodeType `json:"node_type"`
	ClaimModality  string              `json:"claim_modality,omitempty"`
	WriteState     string              `json:"write_state,omitempty"`
	AnchorsJSON    string              `json:"anchors_json,omitempty"`
	MemoryType     string              `json:"memory_type,omitempty"`
	Summary        string              `json:"summary,omitempty"`
	TaskID         string              `json:"task_id,omitempty"`
	SessionID      string              `json:"session_id,omitempty"`
	TensionID      string              `json:"tension_id,omitempty"`
	ProtoClusterID string              `json:"proto_cluster_id,omitempty"`
	CreatedAt      string              `json:"created_at,omitempty"`
	AttemptCount   int                 `json:"attempt_count,omitempty"`
	LastError      string              `json:"last_error,omitempty"`
}

type LocalMemoryControlSnapshot struct {
	Stats                LocalMemoryStats              `json:"stats"`
	Store                LocalMemoryStoreStats         `json:"store"`
	PacketCacheEntries   int                           `json:"packet_cache_entries"`
	LastSequence         int64                         `json:"last_sequence"`
	DocVersionCount      int                           `json:"doc_version_count"`
	ArtifactVersionCount int                           `json:"artifact_version_count"`
	Procedures           []LocalProcedureMemory        `json:"procedures,omitempty"`
	AntiProcedures       []LocalProcedureMemory        `json:"anti_procedures,omitempty"`
	PendingPromotions    []LocalMemoryPromotionSummary `json:"pending_promotions,omitempty"`
}

type agentMemoryState struct {
	Version          int                             `json:"version"`
	WorkspaceID      string                          `json:"workspace_id"`
	AgentID          string                          `json:"agent_id"`
	LastSequence     int64                           `json:"last_sequence"`
	RecentEvents     []LocalMemoryEvent              `json:"recent_events"`
	TaskDigests      map[string]LocalEpisodeDigest   `json:"task_digests,omitempty"`
	TensionDigests   map[string]LocalEpisodeDigest   `json:"tension_digests,omitempty"`
	ClusterDigests   map[string]LocalEpisodeDigest   `json:"cluster_digests,omitempty"`
	Procedures       map[string]LocalProcedureMemory `json:"procedures,omitempty"`
	AntiProcedures   map[string]LocalProcedureMemory `json:"anti_procedures,omitempty"`
	DocVersions      map[string]string               `json:"doc_versions,omitempty"`
	ArtifactVersions map[string]string               `json:"artifact_versions,omitempty"`
	Stats            LocalMemoryStats                `json:"stats"`
}

type MemoryPacketInput struct {
	Task           *WorkspaceTaskRecord
	Session        *AgentSessionStateRecord
	Focus          *RuntimeFocusState
	Hydration      *TaskHydrationBundle
	WorkPacket     *AgentWorkPacket
	CurrentSummary string
}
