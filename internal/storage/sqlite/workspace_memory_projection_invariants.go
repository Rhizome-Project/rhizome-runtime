package sqlite

import (
	"context"
	"fmt"
	"strings"
)

const (
	WorkspaceMemoryProjectionInvariantCurrent = "CURRENT"
	WorkspaceMemoryProjectionInvariantLagging = "LAGGING"
	WorkspaceMemoryProjectionInvariantDrift   = "DRIFT"
	WorkspaceMemoryProjectionInvariantUnknown = "UNKNOWN"

	workspaceMemoryProjectionInvariantSeverityHard = "hard"
	workspaceMemoryProjectionInvariantSeveritySoft = "soft"
)

type WorkspaceMemoryProjectionInvariantIssue struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type WorkspaceMemoryProjectionInvariant struct {
	WorkspaceID          string                                    `json:"workspace_id"`
	MemoryID             string                                    `json:"memory_id"`
	ExpectedNodeID       string                                    `json:"expected_node_id"`
	State                string                                    `json:"state"`
	Summary              string                                    `json:"summary"`
	ProjectionLagState   string                                    `json:"projection_lag_state,omitempty"`
	ProjectionLagMessage string                                    `json:"projection_lag_message,omitempty"`
	NodePresent          bool                                      `json:"node_present"`
	Node                 MemoryGraphNodeRecord                     `json:"node,omitempty"`
	Issues               []WorkspaceMemoryProjectionInvariantIssue `json:"issues,omitempty"`
}

func (s *Store) EvaluateWorkspaceMemoryProjectionInvariant(ctx context.Context, record WorkspaceMemoryRecord, lag MemoryProjectionLagSnapshot) (WorkspaceMemoryProjectionInvariant, error) {
	workspaceID := strings.TrimSpace(record.WorkspaceID)
	memoryID := strings.TrimSpace(record.MemoryID)
	result := WorkspaceMemoryProjectionInvariant{
		WorkspaceID:          workspaceID,
		MemoryID:             memoryID,
		ExpectedNodeID:       memoryGraphNodeID("workspace_memory", memoryID),
		State:                WorkspaceMemoryProjectionInvariantUnknown,
		Summary:              "workspace memory projection invariants could not be resolved",
		ProjectionLagState:   firstNonEmpty(strings.TrimSpace(lag.State), "unknown"),
		ProjectionLagMessage: strings.TrimSpace(lag.Message),
	}
	if workspaceID == "" || memoryID == "" {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("INPUT_MISSING", workspaceMemoryProjectionInvariantSeverityHard, "workspace_id or memory_id is missing for projection invariant evaluation"))
		result.State, result.Summary = classifyWorkspaceMemoryProjectionInvariant(result.Issues, result.ProjectionLagState)
		return result, nil
	}

	candidates, err := s.listWorkspaceMemoryProjectionCandidates(ctx, workspaceID, result.ExpectedNodeID, memoryID)
	if err != nil {
		return result, fmt.Errorf("list workspace memory projection candidates: %w", err)
	}

	var strayCandidates []MemoryGraphNodeRecord
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.MemoryID) == result.ExpectedNodeID {
			result.NodePresent = true
			result.Node = candidate
			continue
		}
		if strings.EqualFold(strings.TrimSpace(candidate.OriginKind), "workspace_memory") &&
			strings.TrimSpace(candidate.OriginID) == memoryID {
			strayCandidates = append(strayCandidates, candidate)
		}
	}

	if len(strayCandidates) > 0 {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue(
			"DUPLICATE_ANCHOR",
			workspaceMemoryProjectionInvariantSeverityHard,
			fmt.Sprintf("found %d competing compatibility anchor(s) for canonical workspace_memory:%s", len(strayCandidates), memoryID),
		))
	}

	if !result.NodePresent {
		if len(strayCandidates) > 0 {
			result.Issues = append(result.Issues, workspaceMemoryProjectionIssue(
				"CONFLICTING_NODE_ID",
				workspaceMemoryProjectionInvariantSeverityHard,
				"derived compatibility anchor exists only under a conflicting memory_graph node id",
			))
		} else {
			result.Issues = append(result.Issues, workspaceMemoryProjectionIssue(
				"MISSING_ANCHOR",
				workspaceMemoryProjectionInvariantSeveritySoft,
				"derived compatibility anchor is missing for canonical workspace_memory",
			))
		}
		result.State, result.Summary = classifyWorkspaceMemoryProjectionInvariant(result.Issues, result.ProjectionLagState)
		return result, nil
	}

	node := result.Node
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return result, fmt.Errorf("load workspace time authority: %w", err)
	}
	applyMemoryGraphRetentionState(&node, authority.ReferenceAt)
	versions, err := s.listMemoryGraphNodeVersions(ctx, workspaceID, node.MemoryID)
	if err != nil {
		return result, fmt.Errorf("load workspace memory projection versions: %w", err)
	}
	driftReport, err := s.buildMemoryGraphDriftReport(ctx, workspaceID, versions)
	if err != nil {
		return result, fmt.Errorf("build workspace memory projection drift report: %w", err)
	}
	node.Drift = memoryGraphEffectiveDrift(node.Drift, driftReport)
	applyMemoryGraphRecoveryState(&node, &driftReport)
	result.Node = node
	if !strings.EqualFold(strings.TrimSpace(node.OriginKind), "workspace_memory") {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("ORIGIN_KIND_MISMATCH", workspaceMemoryProjectionInvariantSeverityHard, "derived anchor origin_kind does not match canonical workspace_memory authority"))
	}
	if strings.TrimSpace(node.OriginID) != memoryID {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("ORIGIN_ID_MISMATCH", workspaceMemoryProjectionInvariantSeverityHard, "derived anchor origin_id does not match canonical workspace_memory identity"))
	}
	if strings.TrimSpace(node.MemoryID) != result.ExpectedNodeID {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("ANCHOR_NODE_ID_MISMATCH", workspaceMemoryProjectionInvariantSeverityHard, "derived anchor memory_id does not match the canonical workspace_memory compatibility node id"))
	}
	if strings.TrimSpace(node.SemanticLineageID) != "workspace_memory:"+memoryID {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("SEMANTIC_LINEAGE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor semantic_lineage_id no longer matches canonical workspace_memory identity"))
	}

	expectedMemoryType := canonicalMemoryTypeFromWorkspaceMemory(record)
	if strings.TrimSpace(node.MemoryType) != expectedMemoryType {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("MEMORY_TYPE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor memory_type no longer matches canonical workspace_memory mapping"))
	}
	if strings.TrimSpace(node.CompatType) != strings.TrimSpace(record.MemoryType) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("COMPAT_TYPE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor compat_type no longer matches canonical workspace_memory type"))
	}
	if strings.TrimSpace(node.LifecycleState) != memoryGraphLifecycleForWorkspaceMemory(record) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("LIFECYCLE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor lifecycle_state no longer matches canonical workspace_memory lifecycle"))
	}
	if strings.TrimSpace(node.MemoryLayer) != expectedWorkspaceMemoryProjectionLayer(record) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("MEMORY_LAYER_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor memory_layer no longer matches canonical workspace_memory residency"))
	}
	if strings.TrimSpace(node.SourceKind) != strings.TrimSpace(record.SourceKind) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("SOURCE_KIND_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor source_kind no longer matches canonical workspace_memory provenance"))
	}
	if strings.TrimSpace(node.SourceID) != strings.TrimSpace(record.SourceID) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("SOURCE_ID_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor source_id no longer matches canonical workspace_memory provenance"))
	}
	if strings.TrimSpace(node.Title) != strings.TrimSpace(record.Title) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("TITLE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor title no longer matches canonical workspace_memory content"))
	}
	if strings.TrimSpace(node.Body) != strings.TrimSpace(record.Body) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("BODY_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor body no longer matches canonical workspace_memory content"))
	}
	if strings.TrimSpace(node.Summary) != strings.TrimSpace(record.Summary) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("SUMMARY_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor summary no longer matches canonical workspace_memory content"))
	}
	if workspaceMemoryArchived(record) != workspaceMemoryNodeArchived(node) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("ARCHIVE_STATE_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor archive presence no longer matches canonical workspace_memory lifecycle"))
	}
	if strings.TrimSpace(node.ArchivedReason) != strings.TrimSpace(record.ArchivedReason) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("ARCHIVED_REASON_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor archived_reason no longer matches canonical workspace_memory lifecycle"))
	}
	if strings.TrimSpace(node.RecoveryReason) != strings.TrimSpace(record.RecoveryReason) {
		result.Issues = append(result.Issues, workspaceMemoryProjectionIssue("RECOVERY_REASON_MISMATCH", workspaceMemoryProjectionInvariantSeveritySoft, "derived anchor recovery_reason no longer matches canonical workspace_memory lifecycle"))
	}

	result.State, result.Summary = classifyWorkspaceMemoryProjectionInvariant(result.Issues, result.ProjectionLagState)
	return result, nil
}

func (s *Store) listWorkspaceMemoryProjectionCandidates(ctx context.Context, workspaceID, expectedNodeID, memoryID string) ([]MemoryGraphNodeRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT n.memory_id, n.workspace_id, n.memory_type, n.compat_type, n.semantic_lineage_id, n.revision, n.protect, n.unresolved,
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
		     ON s.workspace_id = n.workspace_id AND s.memory_id = n.memory_id
		  WHERE n.workspace_id = ?
		    AND (n.memory_id = ? OR (n.origin_kind = 'workspace_memory' AND n.origin_id = ?))
		  ORDER BY CASE WHEN n.memory_id = ? THEN 0 ELSE 1 END, n.updated_at DESC, n.memory_id DESC`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(expectedNodeID),
		strings.TrimSpace(memoryID),
		strings.TrimSpace(expectedNodeID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]MemoryGraphNodeRecord, 0)
	for rows.Next() {
		record, err := scanMemoryGraphNodeRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func workspaceMemoryProjectionIssue(code, severity, message string) WorkspaceMemoryProjectionInvariantIssue {
	return WorkspaceMemoryProjectionInvariantIssue{
		Code:     strings.TrimSpace(code),
		Severity: strings.TrimSpace(severity),
		Message:  strings.TrimSpace(message),
	}
}

func classifyWorkspaceMemoryProjectionInvariant(issues []WorkspaceMemoryProjectionInvariantIssue, lagState string) (string, string) {
	lagState = firstNonEmpty(strings.TrimSpace(lagState), "unknown")
	hasHard := false
	hasSoft := false
	for _, issue := range issues {
		switch strings.TrimSpace(issue.Severity) {
		case workspaceMemoryProjectionInvariantSeverityHard:
			hasHard = true
		case workspaceMemoryProjectionInvariantSeveritySoft:
			hasSoft = true
		}
	}
	switch {
	case hasHard:
		return WorkspaceMemoryProjectionInvariantDrift, "derived compatibility anchor violates canonical workspace_memory authority invariants"
	case lagState == "degraded" && hasSoft:
		return WorkspaceMemoryProjectionInvariantLagging, "derived compatibility anchor is lagging canonical workspace_memory while projection backlog is active"
	case lagState == "degraded":
		return WorkspaceMemoryProjectionInvariantLagging, "derived compatibility anchor currently matches checked invariants, but projection backlog is still active"
	case hasSoft && lagState == "ok":
		return WorkspaceMemoryProjectionInvariantDrift, "derived compatibility anchor diverges after projection backlog settled"
	case hasSoft:
		return WorkspaceMemoryProjectionInvariantUnknown, "derived compatibility anchor mismatch detected but projection lag visibility is not trustworthy"
	case lagState == "unknown":
		return WorkspaceMemoryProjectionInvariantUnknown, "derived compatibility anchor matches checked invariants, but projection lag visibility is unavailable"
	default:
		return WorkspaceMemoryProjectionInvariantCurrent, "derived compatibility anchor matches canonical workspace_memory invariants"
	}
}

func expectedWorkspaceMemoryProjectionLayer(record WorkspaceMemoryRecord) string {
	layer := memoryGraphLayerForType(canonicalMemoryTypeFromWorkspaceMemory(record))
	if workspaceMemoryArchived(record) {
		return "ARCHIVE"
	}
	return layer
}

func workspaceMemoryArchived(record WorkspaceMemoryRecord) bool {
	return record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != ""
}

func workspaceMemoryNodeArchived(record MemoryGraphNodeRecord) bool {
	return record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != ""
}
