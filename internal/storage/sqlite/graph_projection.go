package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// GraphSnapshotRequest defines the parameters for building the Workspace Graph.
type GraphSnapshotRequest struct {
	WorkspaceID string `json:"workspace_id"`
	Mode        string `json:"mode"`
	FocusID     string `json:"focus_id,omitempty"`
	Limit       int    `json:"limit"`
}

// GraphNode properties
type GraphNode struct {
	ID                   string  `json:"id"`
	RefID                string  `json:"ref_id,omitempty"`
	Label                string  `json:"label"`
	Type                 string  `json:"type"`   // human, agent, session, task, dag_node, action, queue_item, proto_cluster, tension, memory_node
	Status               string  `json:"status"` // e.g. ACTIVE, BLOCKED, COMPLETED
	Author               string  `json:"author,omitempty"`
	CreatedAt            string  `json:"created_at,omitempty"`
	Summary              string  `json:"summary,omitempty"`
	MemoryType           string  `json:"memory_type,omitempty"`
	MemoryLayer          string  `json:"memory_layer,omitempty"`
	Visibility           string  `json:"visibility,omitempty"`
	EpistemicStatus      string  `json:"epistemic_status,omitempty"`
	LifecycleState       string  `json:"lifecycle_state,omitempty"`
	CanonicalAuthority   string  `json:"canonical_authority,omitempty"`
	SurfaceAuthority     string  `json:"surface_authority,omitempty"`
	SurfaceRole          string  `json:"surface_role,omitempty"`
	CompatibilityOnly    bool    `json:"compatibility_only,omitempty"`
	OriginKind           string  `json:"origin_kind,omitempty"`
	OriginID             string  `json:"origin_id,omitempty"`
	SourceKind           string  `json:"source_kind,omitempty"`
	SourceID             string  `json:"source_id,omitempty"`
	AgentID              string  `json:"agent_id,omitempty"`
	SessionID            string  `json:"session_id,omitempty"`
	TaskID               string  `json:"task_id,omitempty"`
	Confidence           float64 `json:"confidence,omitempty"`
	Importance           float64 `json:"importance,omitempty"`
	Activation           float64 `json:"activation,omitempty"`
	Drift                float64 `json:"drift,omitempty"`
	SemanticLineageID    string  `json:"semantic_lineage_id,omitempty"`
	RetentionBand        string  `json:"retention_band,omitempty"`
	RetentionPrunable    bool    `json:"retention_prunable,omitempty"`
	Protect              bool    `json:"protect,omitempty"`
	Unresolved           bool    `json:"unresolved,omitempty"`
	RecoveryCandidate    bool    `json:"recovery_candidate,omitempty"`
	RecoveryGuardReason  string  `json:"recovery_guard_reason,omitempty"`
	RecoveryTriggerCount int     `json:"recovery_trigger_count,omitempty"`
}

// GraphEdge properties
type GraphEdge struct {
	Source           string   `json:"source"`
	Target           string   `json:"target"`
	Label            string   `json:"label"`     // owns, runs_session, works_on_task, claims_task...
	Semantics        string   `json:"semantics"` // solid, dashed, animated, affinity, warning, muted
	Authority        string   `json:"authority,omitempty"`
	Strength         float64  `json:"strength,omitempty"`
	FitScore         *float64 `json:"fit_score,omitempty"`
	SemanticDistance *float64 `json:"semantic_distance,omitempty"`
	EvidenceCount    int      `json:"evidence_count,omitempty"`
	SourceModel      string   `json:"source_model,omitempty"`
	HiddenByDefault  bool     `json:"hidden_by_default,omitempty"`
}

// GraphSnapshot represents the full Workspace Graph state at a point in time
type GraphSnapshot struct {
	Nodes         []GraphNode `json:"nodes"`
	Edges         []GraphEdge `json:"edges"`
	Stats         any         `json:"stats,omitempty"`
	GeneratedAt   string      `json:"generated_at"`
	TimeAuthority any         `json:"time_authority"`
	Mode          string      `json:"mode"`
	Focus         string      `json:"focus,omitempty"`
}

type graphTensionProjection struct {
	ID             string
	ProtoClusterID string
	SurfaceScore   int
	EvidenceCount  int
	TaskIDs        []string
}

func normalizeGraphMode(mode string) string {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return "SYSTEM"
	}
	return mode
}

func trimGraphLabel(label string, max int) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return ""
	}
	if max > 0 && len(label) > max {
		return label[:max-3] + "..."
	}
	return label
}

func graphProtoClusterKindLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "task":
		return "Task Cluster"
	case "session":
		return "Session Cluster"
	case "workspace_doc", "doc":
		return "Doc Cluster"
	case "artifact":
		return "Artifact Cluster"
	case "source":
		return "Source Cluster"
	case "claim":
		return "Claim Cluster"
	default:
		return "Proto-Cluster"
	}
}

func graphProtoClusterDisplayLabel(clusterID string, maxDetail int) string {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return ""
	}
	head, tail, ok := strings.Cut(clusterID, ":")
	if !ok || strings.TrimSpace(head) == "" {
		return firstNonEmpty(trimGraphLabel(clusterID, maxDetail), clusterID)
	}

	kindLabel := graphProtoClusterKindLabel(head)
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return kindLabel
	}

	parts := strings.FieldsFunc(tail, func(r rune) bool { return r == '/' })
	switch strings.TrimSpace(head) {
	case "source":
		if len(parts) > 1 {
			tail = strings.Join(parts[1:], "/")
		} else if len(parts) == 1 {
			tail = parts[0]
		}
	default:
		if len(parts) > 1 {
			tail = parts[len(parts)-1]
		} else if len(parts) == 1 {
			tail = parts[0]
		}
	}
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return kindLabel
	}
	if maxDetail > 0 {
		tail = trimGraphLabel(tail, maxDetail)
	}
	return kindLabel + ": " + tail
}

func graphFloatPtr(value float64) *float64 {
	return &value
}

func graphMemoryNodeID(memoryID string) string {
	return "memory:" + strings.TrimSpace(memoryID)
}

func graphMemoryTypeLabel(memoryType string) string {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION_RECORD":
		return "Decision"
	case "BLOCKER_SYMPTOM":
		return "Blocker"
	case "BLOCKER_HYPOTHESIS":
		return "Hypothesis"
	case "ALTERNATIVE_BRANCH":
		return "Alt Branch"
	case "PROCEDURE":
		return "Procedure"
	case "ANTI_PROCEDURE":
		return "Anti-Procedure"
	case "SELF_MODEL":
		return "Self Model"
	case "GOAL_COMMITMENT":
		return "Goal"
	case "POLICY_TRACE":
		return "Policy Trace"
	case "FACT":
		return "Fact"
	case "HANDOFF":
		return "Handoff"
	case "EPISODE_PACK":
		return "Episode"
	case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT":
		return "Dissent"
	case "CONSTRAINT":
		return "Constraint"
	default:
		return firstNonEmpty(strings.ReplaceAll(strings.Title(strings.ToLower(strings.ReplaceAll(strings.TrimSpace(memoryType), "_", " "))), "  ", " "), "Memory")
	}
}

func graphMemoryDisplayLabel(record MemoryGraphNodeRecord) string {
	typeLabel := graphMemoryTypeLabel(record.MemoryType)
	detail := firstNonEmpty(
		strings.TrimSpace(record.Title),
		strings.TrimSpace(record.Summary),
		strings.TrimSpace(record.ClaimSubject),
		strings.TrimSpace(record.MemoryID),
	)
	detail = trimGraphLabel(detail, 30)
	if detail == "" || detail == record.MemoryID {
		return firstNonEmpty(typeLabel, record.MemoryID)
	}
	return firstNonEmpty(typeLabel, "Memory") + ": " + detail
}

func graphMemoryNodeFromRecord(record MemoryGraphNodeRecord) GraphNode {
	return GraphNode{
		ID:                   graphMemoryNodeID(record.MemoryID),
		RefID:                strings.TrimSpace(record.MemoryID),
		Label:                firstNonEmpty(graphMemoryDisplayLabel(record), strings.TrimSpace(record.MemoryID)),
		Type:                 "memory_node",
		Status:               firstNonEmpty(strings.TrimSpace(record.LifecycleState), "ACTIVE"),
		CreatedAt:            firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt)),
		Summary:              trimGraphLabel(firstNonEmpty(strings.TrimSpace(record.Summary), strings.TrimSpace(record.Title)), 140),
		MemoryType:           strings.TrimSpace(record.MemoryType),
		MemoryLayer:          strings.TrimSpace(record.MemoryLayer),
		Visibility:           strings.TrimSpace(record.Visibility),
		EpistemicStatus:      strings.TrimSpace(record.EpistemicStatus),
		LifecycleState:       strings.TrimSpace(record.LifecycleState),
		CanonicalAuthority:   strings.TrimSpace(record.CanonicalAuthority),
		SurfaceAuthority:     strings.TrimSpace(record.SurfaceAuthority),
		SurfaceRole:          strings.TrimSpace(record.SurfaceRole),
		CompatibilityOnly:    record.CompatibilityOnly,
		OriginKind:           strings.TrimSpace(record.OriginKind),
		OriginID:             strings.TrimSpace(record.OriginID),
		SourceKind:           strings.TrimSpace(record.SourceKind),
		SourceID:             strings.TrimSpace(record.SourceID),
		AgentID:              strings.TrimSpace(record.AgentID),
		SessionID:            strings.TrimSpace(record.SessionID),
		TaskID:               strings.TrimSpace(record.TaskID),
		Confidence:           record.Confidence,
		Importance:           record.Importance,
		Activation:           record.Activation,
		Drift:                record.Drift,
		SemanticLineageID:    strings.TrimSpace(record.SemanticLineageID),
		RetentionBand:        strings.TrimSpace(record.RetentionBand),
		RetentionPrunable:    record.RetentionPrunable,
		Protect:              record.Protect,
		Unresolved:           record.Unresolved,
		RecoveryCandidate:    record.RecoveryCandidate,
		RecoveryGuardReason:  strings.TrimSpace(record.RecoveryGuardReason),
		RecoveryTriggerCount: record.RecoveryTriggerCount,
	}
}

func graphMemoryRecordIsWorkspaceMemoryAnchor(record MemoryGraphNodeRecord) bool {
	return strings.EqualFold(strings.TrimSpace(record.OriginKind), "workspace_memory")
}

func graphMemoryNodeSelectSQL() string {
	return `SELECT n.memory_id, n.workspace_id, n.memory_type, n.compat_type, n.semantic_lineage_id, n.revision, n.protect, n.unresolved,
	        s.t_i_acc, s.t_i_star, s.h_i, s.t_hot, s.t_warm, s.t_gc,
	        n.visibility, n.memory_layer,
	        n.epistemic_status, n.lifecycle_state, n.origin_kind, n.origin_id, n.source_kind, n.source_id,
	        n.agent_id, n.session_id, n.task_id, n.title, n.body, n.summary,
	        n.claim_subject, n.claim_predicate, n.claim_object, n.claim_qualifiers_json, n.claim_time_scope_json,
	        n.claim_modality, n.source_set_json, n.provenance_json,
	        n.temperature, n.importance, n.confidence, n.activation, n.drift, n.volatility, n.pin_strength,
	        n.archived_at, n.archived_reason, n.recovery_reason, n.created_at, n.updated_at
	   FROM memory_nodes n
	   LEFT JOIN memory_node_salience s
	     ON s.workspace_id = n.workspace_id AND s.memory_id = n.memory_id`
}

func collectGraphMemoryEdgeRows(rows *sql.Rows) ([]MemoryGraphEdgeRecord, error) {
	out := make([]MemoryGraphEdgeRecord, 0)
	for rows.Next() {
		var record MemoryGraphEdgeRecord
		if err := rows.Scan(
			&record.EdgeID,
			&record.WorkspaceID,
			&record.FromMemoryID,
			&record.ToMemoryID,
			&record.EdgeType,
			&record.SourceKind,
			&record.SourceID,
			&record.Weight,
			&record.MetadataJSON,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan memory graph edge: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory graph edges: %w", err)
	}
	return out, nil
}

func inferPhantomNode(edge GraphEdge, isSource bool) (string, string) {
	if isSource {
		switch edge.Label {
		case "runs_session", "claims_task", "completed_task", "assigned":
			return "agent", "OFFLINE"
		case "awaiting_human":
			return "human", "ACTIVE"
		case "works_on_task":
			return "session", "PHANTOM"
		case "surfaces":
			return "task", "PHANTOM"
		case "pressure_on":
			return "proto_cluster", "PHANTOM"
		case "examines":
			return "session", "PHANTOM"
		case "requires":
			return "tension", "PHANTOM"
		case "blocked_by_action":
			return "task", "PHANTOM"
		case "requested_action":
			return "agent", "OFFLINE"
		}
	} else {
		switch edge.Label {
		case "runs_session":
			return "session", "PHANTOM"
		case "claims_task", "completed_task", "requires", "works_on_task":
			return "task", "PHANTOM"
		case "examines", "assigned", "surfaces", "pressure_on":
			return "tension", "PHANTOM"
		case "blocked_by_action", "requested_action", "awaiting_human":
			return "action", "PENDING"
		}
	}
	return "unknown", "PHANTOM"
}

func safeAgentStatus(status string, lastSeen sql.NullString) string {
	status = strings.TrimSpace(status)
	if status == "" {
		status = "UNKNOWN"
	}
	if !lastSeen.Valid || strings.TrimSpace(lastSeen.String) == "" {
		return "OFFLINE"
	}
	parsed, err := time.Parse(time.RFC3339Nano, lastSeen.String)
	if err != nil {
		return status
	}
	if time.Since(parsed) > 5*time.Minute {
		return "OFFLINE"
	}
	return status
}

// shortGraphSessionID derives a human-distinguishable short form of a session
// id for graph labels. Naive prefix truncation degenerates to the literal
// "session-" for ids like "session-<hash>", so strip well-known id prefixes
// before shortening.
func shortGraphSessionID(sessionID string) string {
	id := strings.TrimSpace(sessionID)
	for _, prefix := range []string{"session-", "sess-", "s-"} {
		if len(id) > len(prefix) && strings.HasPrefix(id, prefix) {
			id = id[len(prefix):]
			break
		}
	}
	if len(id) > 12 {
		id = id[:12]
	}
	if id == "" {
		id = sessionID
	}
	return id
}

func newGraphSnapshot(nowStr string, authority any, mode, focus string, maxRows int) *GraphSnapshot {
	return &GraphSnapshot{
		Nodes:         []GraphNode{},
		Edges:         []GraphEdge{},
		GeneratedAt:   nowStr,
		TimeAuthority: authority,
		Mode:          mode,
		Focus:         strings.TrimSpace(focus),
		Stats: map[string]any{
			"limit":             maxRows,
			"supports_focus":    true,
			"supports_overlay":  false,
			"supports_affinity": true,
		},
	}
}

func graphTaskFocusNodeID(taskID, nodeID string) string {
	return "dag:" + strings.TrimSpace(taskID) + ":" + strings.TrimSpace(nodeID)
}

func graphTaskFocusActionID(actionID string) string {
	return "action:" + strings.TrimSpace(actionID)
}

func graphHasString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func graphPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", count), ",")
}

func (s *Store) buildTaskFocusGraphSnapshot(ctx context.Context, req GraphSnapshotRequest, maxRows int, nowStr string, authority any) (*GraphSnapshot, error) {
	focusTaskID := strings.TrimSpace(req.FocusID)
	snap := newGraphSnapshot(nowStr, authority, "TASK_FOCUS", focusTaskID, maxRows)
	stats := snap.Stats.(map[string]any)
	stats["focus_type"] = "task"

	if focusTaskID == "" {
		stats["focus_required"] = true
		return snap, nil
	}

	task, err := s.GetTaskStatus(ctx, req.WorkspaceID, focusTaskID)
	if err != nil {
		return nil, err
	}

	nodeSeen := make(map[string]struct{})
	edgeSeen := make(map[string]struct{})
	addNode := func(node GraphNode) {
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			return
		}
		if _, exists := nodeSeen[node.ID]; exists {
			return
		}
		nodeSeen[node.ID] = struct{}{}
		snap.Nodes = append(snap.Nodes, node)
	}
	addEdge := func(edge GraphEdge) {
		edge.Source = strings.TrimSpace(edge.Source)
		edge.Target = strings.TrimSpace(edge.Target)
		if edge.Source == "" || edge.Target == "" {
			return
		}
		key := edge.Source + "|" + edge.Target + "|" + strings.TrimSpace(edge.Label)
		if _, exists := edgeSeen[key]; exists {
			return
		}
		edgeSeen[key] = struct{}{}
		snap.Edges = append(snap.Edges, edge)
	}

	addNode(GraphNode{
		ID:        task.TaskID,
		RefID:     task.TaskID,
		Label:     firstNonEmpty(strings.TrimSpace(task.Title), task.TaskID),
		Type:      "task",
		Status:    firstNonEmpty(strings.TrimSpace(task.Status), "PENDING"),
		Author:    strings.TrimSpace(task.OwnerUserID),
		CreatedAt: strings.TrimSpace(task.CreatedAt),
	})

	relatedAgentIDs := make(map[string]struct{})
	relatedClusterIDs := make(map[string]struct{})
	relatedTensionIDs := make(map[string]struct{})
	taskSessionNodeIDs := make(map[string]struct{})

	nodeClaims := make(map[string]WorkspaceNodeRecord)
	if rows, err := s.ListWorkspaceNodes(ctx, WorkspaceNodeFilter{
		WorkspaceID: req.WorkspaceID,
		TaskID:      focusTaskID,
		Limit:       maxRows,
	}); err == nil {
		for _, row := range rows {
			if _, exists := nodeClaims[row.NodeID]; exists {
				continue
			}
			nodeClaims[row.NodeID] = row
		}
	}

	sessionRows, err := s.DB().QueryContext(ctx, `
		SELECT session_id, agent_id, status
		FROM agent_sessions
		WHERE workspace_id = ? AND task_id = ?
		ORDER BY started_at DESC, session_id ASC
		LIMIT ?`, req.WorkspaceID, focusTaskID, maxRows)
	if err != nil {
		return nil, err
	}
	defer sessionRows.Close()

	for sessionRows.Next() {
		var sessionID, agentID, status string
		if err := sessionRows.Scan(&sessionID, &agentID, &status); err != nil {
			return nil, err
		}
		shortSessionID := shortGraphSessionID(sessionID)
		addNode(GraphNode{
			ID:     sessionID,
			RefID:  sessionID,
			Label:  "Session " + shortSessionID,
			Type:   "session",
			Status: firstNonEmpty(strings.TrimSpace(status), "ACTIVE"),
		})
		taskSessionNodeIDs[sessionID] = struct{}{}
		if agentID = strings.TrimSpace(agentID); agentID != "" {
			relatedAgentIDs[agentID] = struct{}{}
			addEdge(GraphEdge{
				Source:    agentID,
				Target:    sessionID,
				Label:     "runs_session",
				Semantics: "solid",
			})
		}
		addEdge(GraphEdge{
			Source:    sessionID,
			Target:    focusTaskID,
			Label:     "works_on_task",
			Semantics: "solid",
		})
	}
	if err := sessionRows.Err(); err != nil {
		return nil, err
	}

	claimRows, err := s.DB().QueryContext(ctx, `
		SELECT agent_id, claim_status
		FROM task_claims
		WHERE workspace_id = ? AND task_id = ? AND claim_status IN ('CLAIMED', 'BLOCKED', 'COMPLETED')
		ORDER BY agent_id`, req.WorkspaceID, focusTaskID)
	if err != nil {
		return nil, err
	}
	defer claimRows.Close()

	for claimRows.Next() {
		var agentID, claimStatus string
		if err := claimRows.Scan(&agentID, &claimStatus); err != nil {
			return nil, err
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		relatedAgentIDs[agentID] = struct{}{}
		label := "claims_task"
		semantics := "solid"
		if strings.EqualFold(claimStatus, "CLAIMED") {
			semantics = "animated"
		} else if strings.EqualFold(claimStatus, "COMPLETED") {
			label = "completed_task"
			semantics = "muted"
		}
		addEdge(GraphEdge{
			Source:    agentID,
			Target:    focusTaskID,
			Label:     label,
			Semantics: semantics,
		})
	}
	if err := claimRows.Err(); err != nil {
		return nil, err
	}

	for _, node := range task.Nodes {
		nodeGraphID := graphTaskFocusNodeID(focusTaskID, node.NodeID)
		status := firstNonEmpty(strings.TrimSpace(node.Status), "PENDING")
		addNode(GraphNode{
			ID:        nodeGraphID,
			RefID:     node.NodeID,
			Label:     firstNonEmpty(trimGraphLabel(node.NodeID, 24), node.NodeID),
			Type:      "dag_node",
			Status:    status,
			CreatedAt: task.UpdatedAt,
		})
		addEdge(GraphEdge{
			Source:    focusTaskID,
			Target:    nodeGraphID,
			Label:     "contains_node",
			Semantics: "solid",
		})
		for _, dependsOn := range node.DependsOn {
			dependsOn = strings.TrimSpace(dependsOn)
			if dependsOn == "" {
				continue
			}
			addEdge(GraphEdge{
				Source:    graphTaskFocusNodeID(focusTaskID, dependsOn),
				Target:    nodeGraphID,
				Label:     "depends_on",
				Semantics: "dashed",
			})
		}
		if claim, ok := nodeClaims[node.NodeID]; ok && claim.ClaimAgentID != nil && strings.TrimSpace(*claim.ClaimAgentID) != "" {
			agentID := strings.TrimSpace(*claim.ClaimAgentID)
			relatedAgentIDs[agentID] = struct{}{}
			claimSemantics := "solid"
			if claim.ClaimStatus != nil && strings.EqualFold(strings.TrimSpace(*claim.ClaimStatus), "CLAIMED") {
				claimSemantics = "animated"
			}
			addEdge(GraphEdge{
				Source:    agentID,
				Target:    nodeGraphID,
				Label:     "claims_node",
				Semantics: claimSemantics,
			})
		}
	}

	actionRows, err := s.DB().QueryContext(ctx, `
		SELECT ha.action_id, ha.agent_id, ha.assigned_to, COALESCE(NULLIF(TRIM(h.display_name), ''), NULLIF(TRIM(h.username), ''), NULLIF(TRIM(ha.assigned_to), ''), ''), ha.title, ha.status, ha.created_at
		FROM human_actions ha
		LEFT JOIN workspace_humans h ON h.workspace_id = ha.workspace_id AND (h.human_id = ha.assigned_to OR h.username = ha.assigned_to)
		WHERE ha.workspace_id = ? AND ha.task_id = ? AND ha.blocking = 1
		ORDER BY CASE ha.status WHEN 'PENDING' THEN 0 WHEN 'COMPLETED' THEN 1 ELSE 2 END, ha.created_at DESC
		LIMIT ?`, req.WorkspaceID, focusTaskID, maxRows)
	if err != nil {
		return nil, err
	}
	defer actionRows.Close()

	for actionRows.Next() {
		var actionID, assignedLabel, title, status, createdAt string
		var agentID, assignedTo sql.NullString
		if err := actionRows.Scan(&actionID, &agentID, &assignedTo, &assignedLabel, &title, &status, &createdAt); err != nil {
			return nil, err
		}
		agentIDValue := strings.TrimSpace(agentID.String)
		assignedToValue := strings.TrimSpace(assignedTo.String)
		graphActionID := graphTaskFocusActionID(actionID)
		addNode(GraphNode{
			ID:        graphActionID,
			RefID:     actionID,
			Label:     firstNonEmpty(trimGraphLabel(title, 26), actionID),
			Type:      "action",
			Status:    firstNonEmpty(strings.TrimSpace(status), "PENDING"),
			Author:    agentIDValue,
			CreatedAt: createdAt,
		})
		addEdge(GraphEdge{
			Source:    focusTaskID,
			Target:    graphActionID,
			Label:     "blocked_by_action",
			Semantics: "warning",
		})
		assignedLabel = firstNonEmpty(strings.TrimSpace(assignedLabel), assignedToValue)
		if assignedToValue != "" {
			humanNodeID := "human:" + assignedToValue
			addNode(GraphNode{
				ID:     humanNodeID,
				RefID:  assignedToValue,
				Label:  firstNonEmpty(trimGraphLabel(assignedLabel, 22), assignedToValue),
				Type:   "human",
				Status: "ACTIVE",
			})
			addEdge(GraphEdge{
				Source:    humanNodeID,
				Target:    graphActionID,
				Label:     "awaiting_human",
				Semantics: "muted",
			})
		} else if agentIDValue != "" {
			relatedAgentIDs[agentIDValue] = struct{}{}
			addEdge(GraphEdge{
				Source:    agentIDValue,
				Target:    graphActionID,
				Label:     "requested_action",
				Semantics: "muted",
			})
		}
	}
	if err := actionRows.Err(); err != nil {
		return nil, err
	}

	type focusTensionProjection struct {
		ID             string
		ProtoClusterID string
		SurfaceScore   int
		EvidenceCount  int
		SessionIDs     []string
		AgentIDs       []string
	}
	tensionProjections := make([]focusTensionProjection, 0)
	tensionRows, err := s.DB().QueryContext(ctx, `
		SELECT tension_id, proto_cluster_id, title, lifecycle_state, surface_score, evidence_count, task_ids_json, session_ids_json, agent_ids_json
		FROM workspace_tensions
		WHERE workspace_id = ? AND lifecycle_state IN ('ACTIVE', 'EMERGENT')
		ORDER BY tension_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err == nil {
		defer tensionRows.Close()
		for tensionRows.Next() {
			var id, protoClusterID, title, state, taskJSON, sessionJSON, agentJSON string
			var surfaceScore, evidenceCount int
			if err := tensionRows.Scan(&id, &protoClusterID, &title, &state, &surfaceScore, &evidenceCount, &taskJSON, &sessionJSON, &agentJSON); err != nil {
				return nil, err
			}
			var taskIDs, sessionIDs, agentIDs []string
			_ = json.Unmarshal([]byte(taskJSON), &taskIDs)
			if !graphHasString(taskIDs, focusTaskID) {
				continue
			}
			_ = json.Unmarshal([]byte(sessionJSON), &sessionIDs)
			_ = json.Unmarshal([]byte(agentJSON), &agentIDs)

			shortTitle := trimGraphLabel(title, 20)
			if shortTitle == "" {
				shortTitle = id
			}
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  "!" + shortTitle,
				Type:   "tension",
				Status: state,
			})
			relatedTensionIDs[id] = struct{}{}
			tensionProjections = append(tensionProjections, focusTensionProjection{
				ID:             id,
				ProtoClusterID: strings.TrimSpace(protoClusterID),
				SurfaceScore:   surfaceScore,
				EvidenceCount:  evidenceCount,
				SessionIDs:     append([]string(nil), sessionIDs...),
				AgentIDs:       append([]string(nil), agentIDs...),
			})
			addEdge(GraphEdge{
				Source:    id,
				Target:    focusTaskID,
				Label:     "requires",
				Semantics: "dashed",
			})
			for _, sessionID := range sessionIDs {
				sessionID = strings.TrimSpace(sessionID)
				if sessionID == "" {
					continue
				}
				if _, exists := taskSessionNodeIDs[sessionID]; exists {
					addEdge(GraphEdge{
						Source:    sessionID,
						Target:    id,
						Label:     "examines",
						Semantics: "dashed",
					})
				}
			}
			for _, agentID := range agentIDs {
				agentID = strings.TrimSpace(agentID)
				if agentID == "" {
					continue
				}
				relatedAgentIDs[agentID] = struct{}{}
				addEdge(GraphEdge{
					Source:    agentID,
					Target:    id,
					Label:     "assigned",
					Semantics: "solid",
				})
			}
		}
		if err := tensionRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(relatedAgentIDs) > 0 {
		agentIDs := make([]string, 0, len(relatedAgentIDs))
		args := []any{req.WorkspaceID}
		for agentID := range relatedAgentIDs {
			agentIDs = append(agentIDs, agentID)
			args = append(args, agentID)
		}
		query := `
			SELECT agent_id, display_name, status, last_seen_at
			FROM agents
			WHERE workspace_id = ? AND agent_id IN (` + graphPlaceholders(len(agentIDs)) + `)
			ORDER BY COALESCE(display_name, agent_id), agent_id`
		agentRows, err := s.DB().QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer agentRows.Close()
		for agentRows.Next() {
			var id, name, status string
			var lastSeen sql.NullString
			if err := agentRows.Scan(&id, &name, &status, &lastSeen); err != nil {
				return nil, err
			}
			name = firstNonEmpty(strings.TrimSpace(name), id)
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  name,
				Type:   "agent",
				Status: safeAgentStatus(status, lastSeen),
			})
		}
		if err := agentRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(tensionProjections) > 0 {
		clusterRows, err := s.listClusterControlStateRows(ctx, req.WorkspaceID, "")
		if err == nil {
			clusterByID := make(map[string]ClusterControlStateRecord, len(clusterRows))
			for _, row := range clusterRows {
				clusterByID[strings.TrimSpace(row.ProtoClusterID)] = row
			}
			for _, tension := range tensionProjections {
				surfaceStrength := clampCoalitionSignal(float64(tension.SurfaceScore) / 100.0)
				if surfaceStrength > 0 {
					addEdge(GraphEdge{
						Source:          focusTaskID,
						Target:          tension.ID,
						Label:           "surfaces",
						Semantics:       "affinity",
						Authority:       "derived",
						Strength:        surfaceStrength,
						EvidenceCount:   tension.EvidenceCount,
						SourceModel:     "surface",
						HiddenByDefault: true,
					})
				}
				clusterID := strings.TrimSpace(tension.ProtoClusterID)
				if clusterID == "" {
					continue
				}
				cluster, ok := clusterByID[clusterID]
				if !ok {
					continue
				}
				if _, exists := relatedClusterIDs[clusterID]; !exists {
					addNode(GraphNode{
						ID:     clusterID,
						RefID:  clusterID,
						Label:  firstNonEmpty(graphProtoClusterDisplayLabel(clusterID, 18), clusterID),
						Type:   "proto_cluster",
						Status: firstNonEmpty(strings.TrimSpace(cluster.AttentionBand), "STEADY"),
					})
					relatedClusterIDs[clusterID] = struct{}{}
				}
				pressureStrength := clampCoalitionSignal(float64(cluster.PressureScore) / 100.0)
				if pressureStrength > 0 {
					addEdge(GraphEdge{
						Source:          clusterID,
						Target:          tension.ID,
						Label:           "pressure_on",
						Semantics:       "affinity",
						Authority:       "derived",
						Strength:        pressureStrength,
						SourceModel:     "control",
						HiddenByDefault: true,
					})
				}
			}
		}

		for agentID := range relatedAgentIDs {
			scored, err := s.ListAgentAvailableTensionsScored(ctx, req.WorkspaceID, agentID)
			if err != nil {
				continue
			}
			added := 0
			for _, candidate := range scored {
				if added >= 3 {
					break
				}
				tensionID := strings.TrimSpace(candidate.TensionID)
				if tensionID == "" {
					continue
				}
				if _, ok := relatedTensionIDs[tensionID]; !ok {
					continue
				}
				if candidate.AttachProb < 0.18 || candidate.AttachFactors.Fit < 0.35 {
					continue
				}
				fitScore := clampCoalitionSignal(candidate.AttachFactors.Fit)
				semanticDistance := 1.0 - fitScore
				addEdge(GraphEdge{
					Source:           agentID,
					Target:           tensionID,
					Label:            "candidate_for",
					Semantics:        "affinity",
					Authority:        "inferred",
					Strength:         clampCoalitionSignal(candidate.AttachProb),
					FitScore:         graphFloatPtr(fitScore),
					SemanticDistance: graphFloatPtr(semanticDistance),
					EvidenceCount:    candidate.EvidenceCount,
					SourceModel:      "attachment",
					HiddenByDefault:  true,
				})
				added++
			}
		}
	}

	knownIDs := make(map[string]bool, len(snap.Nodes))
	for _, node := range snap.Nodes {
		knownIDs[node.ID] = true
	}
	for _, edge := range snap.Edges {
		if !knownIDs[edge.Source] {
			label := trimGraphLabel(edge.Source, 12)
			nodeType, status := inferPhantomNode(edge, true)
			addNode(GraphNode{
				ID:     edge.Source,
				RefID:  edge.Source,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Source] = true
		}
		if !knownIDs[edge.Target] {
			label := trimGraphLabel(edge.Target, 12)
			nodeType, status := inferPhantomNode(edge, false)
			addNode(GraphNode{
				ID:     edge.Target,
				RefID:  edge.Target,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Target] = true
		}
	}

	stats["supports_focus"] = true
	stats["dag_node_count"] = len(task.Nodes)
	stats["focus_task_title"] = task.Title
	return snap, nil
}

func (s *Store) buildControlGraphSnapshot(ctx context.Context, req GraphSnapshotRequest, maxRows int, nowStr string, authority any) (*GraphSnapshot, error) {
	focusClusterID := strings.TrimSpace(req.FocusID)
	snap := newGraphSnapshot(nowStr, authority, "CONTROL", focusClusterID, maxRows)
	stats := snap.Stats.(map[string]any)
	stats["view"] = "control"
	stats["focus_type"] = "proto_cluster"
	if focusClusterID != "" {
		stats["focus_cluster_id"] = focusClusterID
	}

	type graphControlCluster struct {
		ID                  string
		Status              string
		UpdatedAt           string
		PressureScore       int
		CurrentMode         string
		Summary             string
		TaskIDs             []string
		SessionIDs          []string
		AgentIDs            []string
		ConfirmedTensionIDs []string
		PendingTensionIDs   []string
	}

	clusterRows, err := s.listClusterControlStateRows(ctx, req.WorkspaceID, focusClusterID)
	if err != nil {
		return nil, err
	}
	clusters := make([]graphControlCluster, 0, len(clusterRows))
	for _, row := range clusterRows {
		clusterID := strings.TrimSpace(row.ProtoClusterID)
		if clusterID == "" {
			continue
		}
		clusters = append(clusters, graphControlCluster{
			ID:                  clusterID,
			Status:              firstNonEmpty(strings.TrimSpace(row.AttentionBand), "STEADY"),
			UpdatedAt:           strings.TrimSpace(row.UpdatedAt),
			PressureScore:       row.PressureScore,
			CurrentMode:         strings.TrimSpace(row.CurrentMode),
			Summary:             strings.TrimSpace(row.Summary),
			TaskIDs:             append([]string{}, row.TaskIDs...),
			SessionIDs:          append([]string{}, row.SessionIDs...),
			AgentIDs:            append([]string{}, row.AgentIDs...),
			ConfirmedTensionIDs: append([]string{}, row.ConfirmedTensionIDs...),
			PendingTensionIDs:   append([]string{}, row.PendingTensionIDs...),
		})
	}
	if len(clusters) == 0 {
		report, err := s.BuildControlReport(ctx, ControlReportFilter{
			WorkspaceID:    req.WorkspaceID,
			ProtoClusterID: focusClusterID,
			Limit:          maxRows,
		})
		if err != nil {
			return nil, err
		}
		for _, cluster := range report.Clusters {
			clusterID := strings.TrimSpace(cluster.ProtoClusterID)
			if clusterID == "" {
				continue
			}
			clusters = append(clusters, graphControlCluster{
				ID:                  clusterID,
				Status:              firstNonEmpty(strings.TrimSpace(cluster.Signals.AttentionBand), "STEADY"),
				UpdatedAt:           strings.TrimSpace(report.GeneratedAt),
				PressureScore:       cluster.Signals.PressureScore,
				Summary:             strings.TrimSpace(cluster.Summary),
				TaskIDs:             append([]string{}, cluster.TaskIDs...),
				SessionIDs:          append([]string{}, cluster.SessionIDs...),
				AgentIDs:            append([]string{}, cluster.AgentIDs...),
				ConfirmedTensionIDs: append([]string{}, cluster.ConfirmedTensionIDs...),
				PendingTensionIDs:   append([]string{}, cluster.PendingTensionIDs...),
			})
		}
	}
	if len(clusters) == 0 {
		stats["cluster_count"] = 0
		return snap, nil
	}

	nodeSeen := make(map[string]struct{})
	edgeSeen := make(map[string]struct{})
	addNode := func(node GraphNode) {
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			return
		}
		if _, exists := nodeSeen[node.ID]; exists {
			return
		}
		nodeSeen[node.ID] = struct{}{}
		snap.Nodes = append(snap.Nodes, node)
	}
	addEdge := func(edge GraphEdge) {
		edge.Source = strings.TrimSpace(edge.Source)
		edge.Target = strings.TrimSpace(edge.Target)
		if edge.Source == "" || edge.Target == "" {
			return
		}
		key := edge.Source + "|" + edge.Target + "|" + strings.TrimSpace(edge.Label)
		if _, exists := edgeSeen[key]; exists {
			return
		}
		edgeSeen[key] = struct{}{}
		snap.Edges = append(snap.Edges, edge)
	}

	clusterByID := make(map[string]graphControlCluster, len(clusters))
	relatedTaskIDs := make(map[string]struct{})
	relatedSessionIDs := make(map[string]struct{})
	relatedAgentIDs := make(map[string]struct{})
	relatedTensionIDs := make(map[string]struct{})

	for _, cluster := range clusters {
		clusterID := cluster.ID
		clusterByID[clusterID] = cluster
		addNode(GraphNode{
			ID:        clusterID,
			RefID:     clusterID,
			Label:     firstNonEmpty(graphProtoClusterDisplayLabel(clusterID, 18), clusterID),
			Type:      "proto_cluster",
			Status:    firstNonEmpty(strings.TrimSpace(cluster.Status), "STEADY"),
			CreatedAt: strings.TrimSpace(cluster.UpdatedAt),
		})
		for _, taskID := range cluster.TaskIDs {
			taskID = strings.TrimSpace(taskID)
			if taskID == "" {
				continue
			}
			relatedTaskIDs[taskID] = struct{}{}
			addEdge(GraphEdge{
				Source:    clusterID,
				Target:    taskID,
				Label:     "tracks_task",
				Semantics: "solid",
			})
		}
		for _, sessionID := range cluster.SessionIDs {
			sessionID = strings.TrimSpace(sessionID)
			if sessionID == "" {
				continue
			}
			relatedSessionIDs[sessionID] = struct{}{}
			addEdge(GraphEdge{
				Source:    clusterID,
				Target:    sessionID,
				Label:     "observes_session",
				Semantics: "dashed",
			})
		}
		for _, agentID := range cluster.AgentIDs {
			agentID = strings.TrimSpace(agentID)
			if agentID == "" {
				continue
			}
			relatedAgentIDs[agentID] = struct{}{}
			addEdge(GraphEdge{
				Source:    agentID,
				Target:    clusterID,
				Label:     "stewards_cluster",
				Semantics: "dashed",
			})
		}
		for _, tensionID := range append(append([]string{}, cluster.ConfirmedTensionIDs...), cluster.PendingTensionIDs...) {
			if tensionID = strings.TrimSpace(tensionID); tensionID != "" {
				relatedTensionIDs[tensionID] = struct{}{}
			}
		}
	}

	if len(relatedTaskIDs) > 0 {
		args := []any{req.WorkspaceID}
		taskIDs := make([]string, 0, len(relatedTaskIDs))
		for taskID := range relatedTaskIDs {
			taskIDs = append(taskIDs, taskID)
			args = append(args, taskID)
		}
		taskRows, err := s.DB().QueryContext(ctx, `
			SELECT wt.task_id, t.title, t.status
			FROM workspace_tasks wt
			JOIN tasks t ON t.task_id = wt.task_id
			WHERE wt.workspace_id = ? AND wt.task_id IN (`+graphPlaceholders(len(taskIDs))+`)
			ORDER BY wt.task_id`, args...)
		if err != nil {
			return nil, err
		}
		defer taskRows.Close()
		for taskRows.Next() {
			var id, title, status string
			if err := taskRows.Scan(&id, &title, &status); err != nil {
				return nil, err
			}
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  firstNonEmpty(strings.TrimSpace(title), id),
				Type:   "task",
				Status: firstNonEmpty(strings.TrimSpace(status), "PENDING"),
			})
		}
		if err := taskRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(relatedSessionIDs) > 0 {
		args := []any{req.WorkspaceID}
		sessionIDs := make([]string, 0, len(relatedSessionIDs))
		for sessionID := range relatedSessionIDs {
			sessionIDs = append(sessionIDs, sessionID)
			args = append(args, sessionID)
		}
		sessionRows, err := s.DB().QueryContext(ctx, `
			SELECT session_id, agent_id, status
			FROM agent_sessions
			WHERE workspace_id = ? AND session_id IN (`+graphPlaceholders(len(sessionIDs))+`)
			ORDER BY session_id`, args...)
		if err != nil {
			return nil, err
		}
		defer sessionRows.Close()
		for sessionRows.Next() {
			var sessionID, agentID, status string
			if err := sessionRows.Scan(&sessionID, &agentID, &status); err != nil {
				return nil, err
			}
			shortSessionID := shortGraphSessionID(sessionID)
			addNode(GraphNode{
				ID:     sessionID,
				RefID:  sessionID,
				Label:  "Session " + shortSessionID,
				Type:   "session",
				Status: firstNonEmpty(strings.TrimSpace(status), "ACTIVE"),
			})
			if agentID = strings.TrimSpace(agentID); agentID != "" {
				relatedAgentIDs[agentID] = struct{}{}
				addEdge(GraphEdge{
					Source:    agentID,
					Target:    sessionID,
					Label:     "runs_session",
					Semantics: "solid",
				})
			}
		}
		if err := sessionRows.Err(); err != nil {
			return nil, err
		}
	}

	tensionRows, err := s.DB().QueryContext(ctx, `
		SELECT tension_id, proto_cluster_id, title, lifecycle_state, surface_score, evidence_count, task_ids_json, session_ids_json, agent_ids_json
		FROM workspace_tensions
		WHERE workspace_id = ?
		ORDER BY tension_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err == nil {
		defer tensionRows.Close()
		for tensionRows.Next() {
			var id, protoClusterID, title, state, taskJSON, sessionJSON, agentJSON string
			var surfaceScore, evidenceCount int
			if err := tensionRows.Scan(&id, &protoClusterID, &title, &state, &surfaceScore, &evidenceCount, &taskJSON, &sessionJSON, &agentJSON); err != nil {
				return nil, err
			}
			clusterID := strings.TrimSpace(protoClusterID)
			_, clusterReferenced := clusterByID[clusterID]
			_, tensionReferenced := relatedTensionIDs[id]
			if !clusterReferenced && !tensionReferenced {
				continue
			}
			var taskIDs, sessionIDs, agentIDs []string
			_ = json.Unmarshal([]byte(taskJSON), &taskIDs)
			_ = json.Unmarshal([]byte(sessionJSON), &sessionIDs)
			_ = json.Unmarshal([]byte(agentJSON), &agentIDs)

			shortTitle := trimGraphLabel(title, 20)
			if shortTitle == "" {
				shortTitle = id
			}
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  "!" + shortTitle,
				Type:   "tension",
				Status: firstNonEmpty(strings.TrimSpace(state), "ACTIVE"),
			})
			relatedTensionIDs[id] = struct{}{}
			if clusterReferenced {
				pressureStrength := clampCoalitionSignal(float64(clusterByID[clusterID].PressureScore) / 100.0)
				if pressureStrength > 0 {
					addEdge(GraphEdge{
						Source:          clusterID,
						Target:          id,
						Label:           "pressure_on",
						Semantics:       "affinity",
						Authority:       "derived",
						Strength:        pressureStrength,
						SourceModel:     "control",
						HiddenByDefault: true,
					})
				}
			}
			surfaceStrength := clampCoalitionSignal(float64(surfaceScore) / 100.0)
			for _, taskID := range taskIDs {
				taskID = strings.TrimSpace(taskID)
				if taskID == "" {
					continue
				}
				relatedTaskIDs[taskID] = struct{}{}
				addEdge(GraphEdge{
					Source:    id,
					Target:    taskID,
					Label:     "requires",
					Semantics: "dashed",
				})
				if surfaceStrength > 0 {
					addEdge(GraphEdge{
						Source:          taskID,
						Target:          id,
						Label:           "surfaces",
						Semantics:       "affinity",
						Authority:       "derived",
						Strength:        surfaceStrength,
						EvidenceCount:   evidenceCount,
						SourceModel:     "surface",
						HiddenByDefault: true,
					})
				}
			}
			for _, sessionID := range sessionIDs {
				sessionID = strings.TrimSpace(sessionID)
				if sessionID == "" {
					continue
				}
				relatedSessionIDs[sessionID] = struct{}{}
				addEdge(GraphEdge{
					Source:    sessionID,
					Target:    id,
					Label:     "examines",
					Semantics: "dashed",
				})
			}
			for _, agentID := range agentIDs {
				agentID = strings.TrimSpace(agentID)
				if agentID == "" {
					continue
				}
				relatedAgentIDs[agentID] = struct{}{}
				addEdge(GraphEdge{
					Source:    agentID,
					Target:    id,
					Label:     "assigned",
					Semantics: "solid",
				})
			}
		}
		if err := tensionRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(relatedTaskIDs) > 0 {
		args := []any{req.WorkspaceID}
		taskIDs := make([]string, 0, len(relatedTaskIDs))
		for taskID := range relatedTaskIDs {
			taskIDs = append(taskIDs, taskID)
			args = append(args, taskID)
		}
		taskRows, err := s.DB().QueryContext(ctx, `
			SELECT wt.task_id, t.title, t.status
			FROM workspace_tasks wt
			JOIN tasks t ON t.task_id = wt.task_id
			WHERE wt.workspace_id = ? AND wt.task_id IN (`+graphPlaceholders(len(taskIDs))+`)
			ORDER BY wt.task_id`, args...)
		if err != nil {
			return nil, err
		}
		defer taskRows.Close()
		for taskRows.Next() {
			var id, title, status string
			if err := taskRows.Scan(&id, &title, &status); err != nil {
				return nil, err
			}
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  firstNonEmpty(strings.TrimSpace(title), id),
				Type:   "task",
				Status: firstNonEmpty(strings.TrimSpace(status), "PENDING"),
			})
		}
		if err := taskRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(relatedSessionIDs) > 0 {
		args := []any{req.WorkspaceID}
		sessionIDs := make([]string, 0, len(relatedSessionIDs))
		for sessionID := range relatedSessionIDs {
			sessionIDs = append(sessionIDs, sessionID)
			args = append(args, sessionID)
		}
		sessionRows, err := s.DB().QueryContext(ctx, `
			SELECT session_id, agent_id, status
			FROM agent_sessions
			WHERE workspace_id = ? AND session_id IN (`+graphPlaceholders(len(sessionIDs))+`)
			ORDER BY session_id`, args...)
		if err != nil {
			return nil, err
		}
		defer sessionRows.Close()
		for sessionRows.Next() {
			var sessionID, agentID, status string
			if err := sessionRows.Scan(&sessionID, &agentID, &status); err != nil {
				return nil, err
			}
			shortSessionID := shortGraphSessionID(sessionID)
			addNode(GraphNode{
				ID:     sessionID,
				RefID:  sessionID,
				Label:  "Session " + shortSessionID,
				Type:   "session",
				Status: firstNonEmpty(strings.TrimSpace(status), "ACTIVE"),
			})
			if agentID = strings.TrimSpace(agentID); agentID != "" {
				relatedAgentIDs[agentID] = struct{}{}
				addEdge(GraphEdge{
					Source:    agentID,
					Target:    sessionID,
					Label:     "runs_session",
					Semantics: "solid",
				})
			}
		}
		if err := sessionRows.Err(); err != nil {
			return nil, err
		}
	}

	if len(relatedAgentIDs) > 0 {
		args := []any{req.WorkspaceID}
		agentIDs := make([]string, 0, len(relatedAgentIDs))
		for agentID := range relatedAgentIDs {
			agentIDs = append(agentIDs, agentID)
			args = append(args, agentID)
		}
		agentRows, err := s.DB().QueryContext(ctx, `
			SELECT agent_id, display_name, status, last_seen_at
			FROM agents
			WHERE workspace_id = ? AND agent_id IN (`+graphPlaceholders(len(agentIDs))+`)
			ORDER BY COALESCE(display_name, agent_id), agent_id`, args...)
		if err != nil {
			return nil, err
		}
		defer agentRows.Close()
		for agentRows.Next() {
			var id, name, status string
			var lastSeen sql.NullString
			if err := agentRows.Scan(&id, &name, &status, &lastSeen); err != nil {
				return nil, err
			}
			addNode(GraphNode{
				ID:     id,
				RefID:  id,
				Label:  firstNonEmpty(strings.TrimSpace(name), id),
				Type:   "agent",
				Status: safeAgentStatus(status, lastSeen),
			})
		}
		if err := agentRows.Err(); err != nil {
			return nil, err
		}
		for agentID := range relatedAgentIDs {
			scored, err := s.ListAgentAvailableTensionsScored(ctx, req.WorkspaceID, agentID)
			if err != nil {
				continue
			}
			added := 0
			for _, candidate := range scored {
				if added >= 3 {
					break
				}
				tensionID := strings.TrimSpace(candidate.TensionID)
				if tensionID == "" {
					continue
				}
				if _, ok := relatedTensionIDs[tensionID]; !ok {
					continue
				}
				if candidate.AttachProb < 0.18 || candidate.AttachFactors.Fit < 0.35 {
					continue
				}
				fitScore := clampCoalitionSignal(candidate.AttachFactors.Fit)
				semanticDistance := 1.0 - fitScore
				addEdge(GraphEdge{
					Source:           agentID,
					Target:           tensionID,
					Label:            "candidate_for",
					Semantics:        "affinity",
					Authority:        "inferred",
					Strength:         clampCoalitionSignal(candidate.AttachProb),
					FitScore:         graphFloatPtr(fitScore),
					SemanticDistance: graphFloatPtr(semanticDistance),
					EvidenceCount:    candidate.EvidenceCount,
					SourceModel:      "attachment",
					HiddenByDefault:  true,
				})
				added++
			}
		}
	}

	knownIDs := make(map[string]bool, len(snap.Nodes))
	for _, node := range snap.Nodes {
		knownIDs[node.ID] = true
	}
	for _, edge := range snap.Edges {
		if !knownIDs[edge.Source] {
			label := trimGraphLabel(edge.Source, 12)
			nodeType, status := inferPhantomNode(edge, true)
			if nodeType == "proto_cluster" {
				label = firstNonEmpty(graphProtoClusterDisplayLabel(edge.Source, 14), label)
			}
			addNode(GraphNode{
				ID:     edge.Source,
				RefID:  edge.Source,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Source] = true
		}
		if !knownIDs[edge.Target] {
			label := trimGraphLabel(edge.Target, 12)
			nodeType, status := inferPhantomNode(edge, false)
			if nodeType == "proto_cluster" {
				label = firstNonEmpty(graphProtoClusterDisplayLabel(edge.Target, 14), label)
			}
			addNode(GraphNode{
				ID:     edge.Target,
				RefID:  edge.Target,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Target] = true
		}
	}

	stats["cluster_count"] = len(clusters)
	return snap, nil
}

func (s *Store) buildMemoryOverlayGraphSnapshot(ctx context.Context, req GraphSnapshotRequest, maxRows int, nowStr string, authority any) (*GraphSnapshot, error) {
	baseReq := req
	baseReq.Mode = "SYSTEM"
	baseReq.FocusID = ""
	baseReq.Limit = maxRows

	snap, err := s.GetGraphSnapshot(ctx, baseReq)
	if err != nil {
		return nil, err
	}

	snap.Mode = "MEMORY_OVERLAY"
	snap.Focus = ""
	if snap.GeneratedAt == "" {
		snap.GeneratedAt = nowStr
	}
	if snap.TimeAuthority == nil {
		snap.TimeAuthority = authority
	}

	stats, _ := snap.Stats.(map[string]any)
	if stats == nil {
		stats = map[string]any{}
		snap.Stats = stats
	}
	stats["view"] = "memory_overlay"
	stats["supports_focus"] = false
	stats["supports_overlay"] = true
	stats["memory_overlay"] = true

	taskIDs := make([]string, 0)
	sessionIDs := make([]string, 0)
	agentIDs := make([]string, 0)
	existingNodeIDs := make(map[string]struct{}, len(snap.Nodes))
	for _, node := range snap.Nodes {
		existingNodeIDs[strings.TrimSpace(node.ID)] = struct{}{}
		refID := strings.TrimSpace(node.RefID)
		if refID == "" {
			refID = strings.TrimSpace(node.ID)
		}
		switch node.Type {
		case "task":
			if refID != "" {
				taskIDs = append(taskIDs, refID)
			}
		case "session":
			if refID != "" {
				sessionIDs = append(sessionIDs, refID)
			}
		case "agent":
			if refID != "" {
				agentIDs = append(agentIDs, refID)
			}
		}
	}
	anchoredRecords := make([]MemoryGraphNodeRecord, 0)
	if len(taskIDs) > 0 || len(sessionIDs) > 0 || len(agentIDs) > 0 {
		query := strings.Builder{}
		query.WriteString(graphMemoryNodeSelectSQL())
		query.WriteString(` WHERE n.workspace_id = ? AND n.origin_kind = 'workspace_memory' AND n.lifecycle_state IN ('ACTIVE', 'DORMANT', 'SUPERSEDED')`)
		args := []any{req.WorkspaceID}
		anchorClauses := make([]string, 0, 3)
		if len(taskIDs) > 0 {
			anchorClauses = append(anchorClauses, `n.task_id IN (`+graphPlaceholders(len(taskIDs))+`)`)
			for _, taskID := range taskIDs {
				args = append(args, taskID)
			}
		}
		if len(sessionIDs) > 0 {
			anchorClauses = append(anchorClauses, `n.session_id IN (`+graphPlaceholders(len(sessionIDs))+`)`)
			for _, sessionID := range sessionIDs {
				args = append(args, sessionID)
			}
		}
		if len(agentIDs) > 0 {
			anchorClauses = append(anchorClauses, `n.agent_id IN (`+graphPlaceholders(len(agentIDs))+`)`)
			for _, agentID := range agentIDs {
				args = append(args, agentID)
			}
		}
		query.WriteString(` AND (` + strings.Join(anchorClauses, ` OR `) + `)`)
		query.WriteString(` ORDER BY CASE n.lifecycle_state WHEN 'ACTIVE' THEN 0 WHEN 'DORMANT' THEN 1 ELSE 2 END, n.updated_at DESC, n.importance DESC, n.memory_id DESC LIMIT ?`)
		args = append(args, maxRows)

		memoryRows, err := s.DB().QueryContext(ctx, query.String(), args...)
		if err != nil {
			return nil, err
		}
		anchoredRecords, err = collectMemoryGraphNodeRows(memoryRows)
		_ = memoryRows.Close()
		if err != nil {
			return nil, err
		}
	}

	nodeSeen := make(map[string]struct{}, len(snap.Nodes))
	for _, node := range snap.Nodes {
		nodeSeen[strings.TrimSpace(node.ID)] = struct{}{}
	}
	edgeSeen := make(map[string]struct{}, len(snap.Edges))
	for _, edge := range snap.Edges {
		key := strings.TrimSpace(edge.Source) + "|" + strings.TrimSpace(edge.Target) + "|" + strings.TrimSpace(edge.Label)
		edgeSeen[key] = struct{}{}
	}
	addNode := func(node GraphNode) {
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			return
		}
		if _, exists := nodeSeen[node.ID]; exists {
			return
		}
		nodeSeen[node.ID] = struct{}{}
		snap.Nodes = append(snap.Nodes, node)
	}
	addEdge := func(edge GraphEdge) {
		edge.Source = strings.TrimSpace(edge.Source)
		edge.Target = strings.TrimSpace(edge.Target)
		if edge.Source == "" || edge.Target == "" {
			return
		}
		key := edge.Source + "|" + edge.Target + "|" + strings.TrimSpace(edge.Label)
		if _, exists := edgeSeen[key]; exists {
			return
		}
		edgeSeen[key] = struct{}{}
		snap.Edges = append(snap.Edges, edge)
	}

	memoryIDs := make([]string, 0, len(anchoredRecords))
	memoryIDSet := make(map[string]struct{}, len(anchoredRecords))
	addAnchorEdges := func(record MemoryGraphNodeRecord) {
		memoryNodeID := graphMemoryNodeID(record.MemoryID)
		if taskID := strings.TrimSpace(record.TaskID); taskID != "" {
			if _, exists := existingNodeIDs[taskID]; exists {
				addEdge(GraphEdge{
					Source:    taskID,
					Target:    memoryNodeID,
					Label:     "anchors_memory",
					Semantics: "solid",
					Authority: "authoritative",
				})
			}
		}
		if sessionID := strings.TrimSpace(record.SessionID); sessionID != "" {
			if _, exists := existingNodeIDs[sessionID]; exists {
				addEdge(GraphEdge{
					Source:    sessionID,
					Target:    memoryNodeID,
					Label:     "emits_memory",
					Semantics: "dashed",
					Authority: "authoritative",
				})
			}
		}
		if agentID := strings.TrimSpace(record.AgentID); agentID != "" {
			if _, exists := existingNodeIDs[agentID]; exists {
				addEdge(GraphEdge{
					Source:    agentID,
					Target:    memoryNodeID,
					Label:     "holds_memory",
					Semantics: "dashed",
					Authority: "authoritative",
				})
			}
		}
	}
	addMemoryRecord := func(record MemoryGraphNodeRecord) {
		memoryID := strings.TrimSpace(record.MemoryID)
		if memoryID == "" {
			return
		}
		if _, exists := memoryIDSet[memoryID]; !exists {
			memoryIDSet[memoryID] = struct{}{}
			memoryIDs = append(memoryIDs, memoryID)
		}
		addNode(graphMemoryNodeFromRecord(record))
		addAnchorEdges(record)
	}
	for _, record := range anchoredRecords {
		addMemoryRecord(record)
	}

	minOverlayNodes := 12
	if len(memoryIDs) < minOverlayNodes {
		recentLimit := maxRows
		if recentLimit < minOverlayNodes {
			recentLimit = minOverlayNodes
		}
		recentRecords, err := s.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{
			WorkspaceID: req.WorkspaceID,
			Limit:       recentLimit,
		})
		if err != nil {
			return nil, err
		}
		for _, record := range recentRecords {
			if len(memoryIDs) >= recentLimit {
				break
			}
			if !graphMemoryRecordIsWorkspaceMemoryAnchor(record) {
				continue
			}
			addMemoryRecord(record)
		}
	}

	if len(memoryIDs) == 0 {
		stats["memory_node_count"] = 0
		stats["memory_edge_count"] = 0
		return snap, nil
	}

	if len(memoryIDs) > 0 {
		edgeQuery := `
			SELECT edge_id, workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id, weight, metadata_json, created_at, updated_at
			FROM memory_edges
			WHERE workspace_id = ? AND (from_memory_id IN (` + graphPlaceholders(len(memoryIDs)) + `) OR to_memory_id IN (` + graphPlaceholders(len(memoryIDs)) + `))
			ORDER BY updated_at DESC, edge_id DESC
			LIMIT ?`
		edgeArgs := make([]any, 0, 2+len(memoryIDs)*2)
		edgeArgs = append(edgeArgs, req.WorkspaceID)
		for _, memoryID := range memoryIDs {
			edgeArgs = append(edgeArgs, memoryID)
		}
		for _, memoryID := range memoryIDs {
			edgeArgs = append(edgeArgs, memoryID)
		}
		edgeArgs = append(edgeArgs, maxRows*2)
		edgeRows, err := s.DB().QueryContext(ctx, edgeQuery, edgeArgs...)
		if err != nil {
			return nil, err
		}
		memoryEdges, err := collectGraphMemoryEdgeRows(edgeRows)
		_ = edgeRows.Close()
		if err != nil {
			return nil, err
		}

		missingNeighborIDs := make([]string, 0)
		missingNeighborSet := make(map[string]struct{})
		for _, edge := range memoryEdges {
			fromID := strings.TrimSpace(edge.FromMemoryID)
			toID := strings.TrimSpace(edge.ToMemoryID)
			if fromID != "" {
				if _, exists := memoryIDSet[fromID]; !exists {
					if _, seen := missingNeighborSet[fromID]; !seen {
						missingNeighborSet[fromID] = struct{}{}
						missingNeighborIDs = append(missingNeighborIDs, fromID)
					}
				}
			}
			if toID != "" {
				if _, exists := memoryIDSet[toID]; !exists {
					if _, seen := missingNeighborSet[toID]; !seen {
						missingNeighborSet[toID] = struct{}{}
						missingNeighborIDs = append(missingNeighborIDs, toID)
					}
				}
			}
		}

		if len(missingNeighborIDs) > 0 {
			remaining := maxRows - len(memoryIDs)
			if remaining < len(missingNeighborIDs) {
				missingNeighborIDs = missingNeighborIDs[:remaining]
			}
			if len(missingNeighborIDs) > 0 {
				neighborQuery := graphMemoryNodeSelectSQL() +
					` WHERE n.workspace_id = ? AND n.origin_kind = 'workspace_memory' AND n.memory_id IN (` + graphPlaceholders(len(missingNeighborIDs)) + `)` +
					` ORDER BY n.updated_at DESC, n.importance DESC, n.memory_id DESC`
				neighborArgs := make([]any, 0, 1+len(missingNeighborIDs))
				neighborArgs = append(neighborArgs, req.WorkspaceID)
				for _, memoryID := range missingNeighborIDs {
					neighborArgs = append(neighborArgs, memoryID)
				}
				neighborRows, err := s.DB().QueryContext(ctx, neighborQuery, neighborArgs...)
				if err != nil {
					return nil, err
				}
				neighborRecords, err := collectMemoryGraphNodeRows(neighborRows)
				_ = neighborRows.Close()
				if err != nil {
					return nil, err
				}
				for _, record := range neighborRecords {
					if !graphMemoryRecordIsWorkspaceMemoryAnchor(record) {
						continue
					}
					memoryID := strings.TrimSpace(record.MemoryID)
					if memoryID == "" {
						continue
					}
					if _, exists := memoryIDSet[memoryID]; !exists {
						memoryIDSet[memoryID] = struct{}{}
						memoryIDs = append(memoryIDs, memoryID)
					}
					addNode(graphMemoryNodeFromRecord(record))
				}
			}
		}

		for _, edge := range memoryEdges {
			fromID := graphMemoryNodeID(edge.FromMemoryID)
			toID := graphMemoryNodeID(edge.ToMemoryID)
			if _, exists := nodeSeen[fromID]; !exists {
				continue
			}
			if _, exists := nodeSeen[toID]; !exists {
				continue
			}
			label := strings.ToLower(strings.TrimSpace(edge.EdgeType))
			if label == "" {
				label = "relates_to"
			}
			addEdge(GraphEdge{
				Source:    fromID,
				Target:    toID,
				Label:     label,
				Semantics: "dashed",
				Authority: "authoritative",
				Strength:  clampCoalitionSignal(edge.Weight),
				SourceModel: firstNonEmpty(
					strings.TrimSpace(edge.SourceKind),
					"memory",
				),
			})
		}
	}

	memoryNodeCount := 0
	memoryEdgeCount := 0
	for _, node := range snap.Nodes {
		if node.Type == "memory_node" {
			memoryNodeCount++
		}
	}
	for _, edge := range snap.Edges {
		if strings.HasPrefix(strings.TrimSpace(edge.Source), "memory:") || strings.HasPrefix(strings.TrimSpace(edge.Target), "memory:") {
			memoryEdgeCount++
		}
	}
	stats["memory_node_count"] = memoryNodeCount
	stats["memory_edge_count"] = memoryEdgeCount
	stats["memory_canonical_only"] = true
	return snap, nil
}

// GetGraphSnapshot directly projects the workspace graph without cascading application logic.
func (s *Store) GetGraphSnapshot(ctx context.Context, req GraphSnapshotRequest) (*GraphSnapshot, error) {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	mode := normalizeGraphMode(req.Mode)
	maxRows := req.Limit
	if maxRows <= 0 {
		maxRows = 1000
	}

	var authority any = nowStr
	if auth, err := s.GetWorkspaceTimeAuthority(ctx, req.WorkspaceID); err == nil && auth.WorkspaceID != "" {
		authority = auth
	}

	if mode == "TASK_FOCUS" {
		return s.buildTaskFocusGraphSnapshot(ctx, req, maxRows, nowStr, authority)
	}
	if mode == "CONTROL" {
		return s.buildControlGraphSnapshot(ctx, req, maxRows, nowStr, authority)
	}
	if mode == "MEMORY_OVERLAY" {
		return s.buildMemoryOverlayGraphSnapshot(ctx, req, maxRows, nowStr, authority)
	}

	snap := newGraphSnapshot(nowStr, authority, mode, req.FocusID, maxRows)

	agentRows, err := s.DB().QueryContext(ctx, `
		SELECT agent_id, display_name, status, last_seen_at
		FROM agents
		WHERE workspace_id = ?
		ORDER BY COALESCE(display_name, agent_id), agent_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err != nil {
		return nil, err
	}
	defer agentRows.Close()

	for agentRows.Next() {
		var id, name, status string
		var lastSeen sql.NullString
		if err := agentRows.Scan(&id, &name, &status, &lastSeen); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			name = id
		}
		snap.Nodes = append(snap.Nodes, GraphNode{
			ID:     id,
			Label:  name,
			Type:   "agent",
			Status: safeAgentStatus(status, lastSeen),
		})
	}
	if err := agentRows.Err(); err != nil {
		return nil, err
	}

	taskRows, err := s.DB().QueryContext(ctx, `
		SELECT wt.task_id, t.title, t.status
		FROM workspace_tasks wt
		JOIN tasks t ON t.task_id = wt.task_id
		WHERE wt.workspace_id = ?
		ORDER BY wt.task_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err != nil {
		return nil, err
	}
	defer taskRows.Close()

	for taskRows.Next() {
		var id, title, status string
		if err := taskRows.Scan(&id, &title, &status); err != nil {
			return nil, err
		}
		title = strings.TrimSpace(title)
		if title == "" {
			title = id
		}
		snap.Nodes = append(snap.Nodes, GraphNode{
			ID:     id,
			Label:  title,
			Type:   "task",
			Status: status,
		})
	}
	if err := taskRows.Err(); err != nil {
		return nil, err
	}

	sessionRows, err := s.DB().QueryContext(ctx, `
		SELECT session_id, agent_id, task_id, status
		FROM agent_sessions
		WHERE workspace_id = ?
		ORDER BY session_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err != nil {
		return nil, err
	}
	defer sessionRows.Close()

	for sessionRows.Next() {
		var sessionID, status string
		var agentID, taskID sql.NullString
		if err := sessionRows.Scan(&sessionID, &agentID, &taskID, &status); err != nil {
			return nil, err
		}
		agentIDValue := strings.TrimSpace(agentID.String)
		taskIDValue := strings.TrimSpace(taskID.String)
		shortSessionID := shortGraphSessionID(sessionID)
		snap.Nodes = append(snap.Nodes, GraphNode{
			ID:     sessionID,
			Label:  "Session " + shortSessionID,
			Type:   "session",
			Status: status,
		})
		if agentIDValue != "" {
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:    agentIDValue,
				Target:    sessionID,
				Label:     "runs_session",
				Semantics: "solid",
			})
		}
		if taskIDValue != "" {
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:    sessionID,
				Target:    taskIDValue,
				Label:     "works_on_task",
				Semantics: "solid",
			})
		}
	}
	if err := sessionRows.Err(); err != nil {
		return nil, err
	}

	claimRows, err := s.DB().QueryContext(ctx, `
		SELECT agent_id, task_id, claim_status
		FROM task_claims
		WHERE workspace_id = ? AND claim_status IN ('CLAIMED', 'BLOCKED', 'COMPLETED')
		ORDER BY agent_id, task_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err != nil {
		return nil, err
	}
	defer claimRows.Close()

	for claimRows.Next() {
		var agentID, taskID, claimStatus string
		if err := claimRows.Scan(&agentID, &taskID, &claimStatus); err != nil {
			return nil, err
		}
		label := "claims_task"
		semantics := "solid"
		if strings.EqualFold(claimStatus, "CLAIMED") {
			semantics = "animated"
		} else if strings.EqualFold(claimStatus, "COMPLETED") {
			// finished work stays visible as a faded structural link
			label = "completed_task"
			semantics = "muted"
		}
		snap.Edges = append(snap.Edges, GraphEdge{
			Source:    agentID,
			Target:    taskID,
			Label:     label,
			Semantics: semantics,
		})
	}
	if err := claimRows.Err(); err != nil {
		return nil, err
	}

	actionRows, err := s.DB().QueryContext(ctx, `
		SELECT ha.action_id, ha.task_id, ha.agent_id, ha.assigned_to, COALESCE(NULLIF(TRIM(h.display_name), ''), NULLIF(TRIM(h.username), ''), NULLIF(TRIM(ha.assigned_to), ''), ''), ha.title, ha.status, ha.created_at
		FROM human_actions ha
		LEFT JOIN workspace_humans h ON h.workspace_id = ha.workspace_id AND (h.human_id = ha.assigned_to OR h.username = ha.assigned_to)
		WHERE ha.workspace_id = ? AND ha.blocking = 1 AND ha.status = 'PENDING'
		ORDER BY ha.created_at DESC, ha.action_id ASC
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err != nil {
		return nil, err
	}
	defer actionRows.Close()
	seenHumanNodeIDs := make(map[string]struct{})

	for actionRows.Next() {
		var actionID, assignedLabel, title, status, createdAt string
		var taskID, agentID, assignedTo sql.NullString
		if err := actionRows.Scan(&actionID, &taskID, &agentID, &assignedTo, &assignedLabel, &title, &status, &createdAt); err != nil {
			return nil, err
		}
		taskIDValue := strings.TrimSpace(taskID.String)
		agentIDValue := strings.TrimSpace(agentID.String)
		assignedToValue := strings.TrimSpace(assignedTo.String)
		graphActionID := graphTaskFocusActionID(actionID)
		snap.Nodes = append(snap.Nodes, GraphNode{
			ID:        graphActionID,
			RefID:     actionID,
			Label:     firstNonEmpty(trimGraphLabel(title, 26), actionID),
			Type:      "action",
			Status:    firstNonEmpty(strings.TrimSpace(status), "PENDING"),
			Author:    agentIDValue,
			CreatedAt: createdAt,
		})
		if taskIDValue != "" {
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:    taskIDValue,
				Target:    graphActionID,
				Label:     "blocked_by_action",
				Semantics: "warning",
			})
		}
		assignedLabel = firstNonEmpty(strings.TrimSpace(assignedLabel), assignedToValue)
		if assignedToValue != "" {
			humanNodeID := "human:" + assignedToValue
			if _, exists := seenHumanNodeIDs[humanNodeID]; !exists {
				snap.Nodes = append(snap.Nodes, GraphNode{
					ID:     humanNodeID,
					RefID:  assignedToValue,
					Label:  firstNonEmpty(trimGraphLabel(assignedLabel, 22), assignedToValue),
					Type:   "human",
					Status: "ACTIVE",
				})
				seenHumanNodeIDs[humanNodeID] = struct{}{}
			}
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:    humanNodeID,
				Target:    graphActionID,
				Label:     "awaiting_human",
				Semantics: "muted",
			})
		} else if agentIDValue != "" {
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:    agentIDValue,
				Target:    graphActionID,
				Label:     "requested_action",
				Semantics: "muted",
			})
		}
	}
	if err := actionRows.Err(); err != nil {
		return nil, err
	}

	tensionRows, err := s.DB().QueryContext(ctx, `
		SELECT tension_id, proto_cluster_id, title, lifecycle_state, surface_score, evidence_count, task_ids_json, session_ids_json, agent_ids_json
		FROM workspace_tensions
		WHERE workspace_id = ? AND lifecycle_state IN ('ACTIVE', 'EMERGENT')
		ORDER BY tension_id
		LIMIT ?`, req.WorkspaceID, maxRows)
	if err == nil {
		defer tensionRows.Close()
		tensionProjections := make([]graphTensionProjection, 0)

		for tensionRows.Next() {
			var id, protoClusterID, title, state, taskJSON, sessionJSON, agentJSON string
			var surfaceScore, evidenceCount int
			if err := tensionRows.Scan(&id, &protoClusterID, &title, &state, &surfaceScore, &evidenceCount, &taskJSON, &sessionJSON, &agentJSON); err != nil {
				return nil, err
			}
			shortTitle := trimGraphLabel(title, 20)
			if shortTitle == "" {
				shortTitle = id
			}
			snap.Nodes = append(snap.Nodes, GraphNode{
				ID:     id,
				Label:  "!" + shortTitle,
				Type:   "tension",
				Status: state,
			})

			var taskIDs, sessionIDs, agentIDs []string
			_ = json.Unmarshal([]byte(taskJSON), &taskIDs)
			_ = json.Unmarshal([]byte(sessionJSON), &sessionIDs)
			_ = json.Unmarshal([]byte(agentJSON), &agentIDs)
			tensionProjections = append(tensionProjections, graphTensionProjection{
				ID:             id,
				ProtoClusterID: strings.TrimSpace(protoClusterID),
				SurfaceScore:   surfaceScore,
				EvidenceCount:  evidenceCount,
				TaskIDs:        append([]string(nil), taskIDs...),
			})

			for _, taskID := range taskIDs {
				if strings.TrimSpace(taskID) == "" {
					continue
				}
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:    id,
					Target:    taskID,
					Label:     "requires",
					Semantics: "dashed",
				})
			}
			for _, sessionID := range sessionIDs {
				if strings.TrimSpace(sessionID) == "" {
					continue
				}
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:    sessionID,
					Target:    id,
					Label:     "examines",
					Semantics: "dashed",
				})
			}
			for _, agentID := range agentIDs {
				if strings.TrimSpace(agentID) == "" {
					continue
				}
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:    agentID,
					Target:    id,
					Label:     "assigned",
					Semantics: "solid",
				})
			}
		}
		if err := tensionRows.Err(); err != nil {
			return nil, err
		}

		clusterRows, err := s.listClusterControlStateRows(ctx, req.WorkspaceID, "")
		if err == nil {
			clusterByID := make(map[string]ClusterControlStateRecord, len(clusterRows))
			for _, row := range clusterRows {
				clusterByID[strings.TrimSpace(row.ProtoClusterID)] = row
			}
			seenClusterNodes := make(map[string]struct{})
			for _, tension := range tensionProjections {
				surfaceStrength := clampCoalitionSignal(float64(tension.SurfaceScore) / 100.0)
				for _, taskID := range tension.TaskIDs {
					taskID = strings.TrimSpace(taskID)
					if taskID == "" || surfaceStrength <= 0 {
						continue
					}
					snap.Edges = append(snap.Edges, GraphEdge{
						Source:          taskID,
						Target:          tension.ID,
						Label:           "surfaces",
						Semantics:       "affinity",
						Authority:       "derived",
						Strength:        surfaceStrength,
						EvidenceCount:   tension.EvidenceCount,
						SourceModel:     "surface",
						HiddenByDefault: true,
					})
				}
				clusterID := strings.TrimSpace(tension.ProtoClusterID)
				if clusterID == "" {
					continue
				}
				cluster, ok := clusterByID[clusterID]
				if !ok {
					continue
				}
				if _, seen := seenClusterNodes[clusterID]; !seen {
					label := graphProtoClusterDisplayLabel(clusterID, 18)
					if label == "" {
						label = clusterID
					}
					snap.Nodes = append(snap.Nodes, GraphNode{
						ID:     clusterID,
						Label:  label,
						Type:   "proto_cluster",
						Status: firstNonEmpty(strings.TrimSpace(cluster.AttentionBand), "STEADY"),
					})
					seenClusterNodes[clusterID] = struct{}{}
				}
				pressureStrength := clampCoalitionSignal(float64(cluster.PressureScore) / 100.0)
				if pressureStrength <= 0 {
					continue
				}
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:          clusterID,
					Target:          tension.ID,
					Label:           "pressure_on",
					Semantics:       "affinity",
					Authority:       "derived",
					Strength:        pressureStrength,
					SourceModel:     "control",
					HiddenByDefault: true,
				})
			}
		}
	}

	agentIDs := make([]string, 0, len(snap.Nodes))
	tensionIDs := make(map[string]struct{})
	for _, node := range snap.Nodes {
		switch node.Type {
		case "agent":
			agentIDs = append(agentIDs, node.ID)
		case "tension":
			tensionIDs[node.ID] = struct{}{}
		}
	}

	const (
		graphAffinityTopK    = 3
		graphAffinityMinProb = 0.18
		graphAffinityMinFit  = 0.35
	)

	for _, agentID := range agentIDs {
		scored, err := s.ListAgentAvailableTensionsScored(ctx, req.WorkspaceID, agentID)
		if err != nil {
			continue
		}
		added := 0
		for _, candidate := range scored {
			if added >= graphAffinityTopK {
				break
			}
			tensionID := strings.TrimSpace(candidate.TensionID)
			if tensionID == "" {
				continue
			}
			if _, ok := tensionIDs[tensionID]; !ok {
				continue
			}
			if candidate.AttachProb < graphAffinityMinProb || candidate.AttachFactors.Fit < graphAffinityMinFit {
				continue
			}
			fitScore := clampCoalitionSignal(candidate.AttachFactors.Fit)
			semanticDistance := 1.0 - fitScore
			snap.Edges = append(snap.Edges, GraphEdge{
				Source:           agentID,
				Target:           tensionID,
				Label:            "candidate_for",
				Semantics:        "affinity",
				Authority:        "inferred",
				Strength:         clampCoalitionSignal(candidate.AttachProb),
				FitScore:         graphFloatPtr(fitScore),
				SemanticDistance: graphFloatPtr(semanticDistance),
				EvidenceCount:    candidate.EvidenceCount,
				SourceModel:      "attachment",
				HiddenByDefault:  true,
			})
			added++
		}
	}

	knownIDs := make(map[string]bool, len(snap.Nodes))
	for _, node := range snap.Nodes {
		knownIDs[node.ID] = true
	}
	for _, edge := range snap.Edges {
		if !knownIDs[edge.Source] {
			label := trimGraphLabel(edge.Source, 12)
			nodeType, status := inferPhantomNode(edge, true)
			if nodeType == "proto_cluster" {
				label = firstNonEmpty(graphProtoClusterDisplayLabel(edge.Source, 14), label)
			}
			snap.Nodes = append(snap.Nodes, GraphNode{
				ID:     edge.Source,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Source] = true
		}
		if !knownIDs[edge.Target] {
			label := trimGraphLabel(edge.Target, 12)
			nodeType, status := inferPhantomNode(edge, false)
			if nodeType == "proto_cluster" {
				label = firstNonEmpty(graphProtoClusterDisplayLabel(edge.Target, 14), label)
			}
			snap.Nodes = append(snap.Nodes, GraphNode{
				ID:     edge.Target,
				Label:  label,
				Type:   nodeType,
				Status: status,
			})
			knownIDs[edge.Target] = true
		}
	}

	return snap, nil
}
