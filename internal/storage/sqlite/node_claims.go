package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/orchestrator"
)

var (
	ErrNodeClaimConflict = errors.New("node already claimed by another agent")
	ErrNodeNotPending    = errors.New("node is not in PENDING status")
)

func (s *Store) appendNodeLifecycleRuntimeEventWithAuthorityTx(
	ctx context.Context,
	tx *sql.Tx,
	authority WorkspaceAuthorityRecord,
	workspaceID string,
	input TaskNodeLifecycleRuntimeEventInput,
	now string,
) (RuntimeEventRecord, error) {
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   input.EventType,
		EntityType:  "dag_node",
		EntityID:    input.NodeID,
		ActorType:   "agent",
		ActorID:     input.AgentID,
		AgentID:     input.AgentID,
		TaskID:      input.TaskID,
		PayloadJSON: mustJSON(input.Payload),
		CreatedAt:   now,
	})
}

type TaskNodeLifecycleRuntimeEventInput struct {
	TaskID    string
	NodeID    string
	AgentID   string
	EventType string
	Payload   map[string]any
}

func (s *Store) ClaimNodeWithEvent(ctx context.Context, input NodeClaimInput) (RuntimeEventRecord, error) {
	return s.claimNodeWithEvent(ctx, input)
}

func (s *Store) ClaimNode(ctx context.Context, input NodeClaimInput) error {
	_, err := s.claimNodeWithEvent(ctx, input)
	return err
}

func (s *Store) claimNodeWithEvent(ctx context.Context, input NodeClaimInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return RuntimeEventRecord{}, errors.New("task_id is required")
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID == "" {
		return RuntimeEventRecord{}, errors.New("node_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin node claim tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var runtimeEvent RuntimeEventRecord

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
			return err
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		}

		var nodeStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`, taskID, nodeID).Scan(&nodeStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNodeNotFound
			}
			return fmt.Errorf("query dag_nodes: %w", err)
		}
		if nodeStatus != model.NodeStatusPending {
			return ErrNodeNotPending
		}

		var currentAgentID, currentClaimStatus string
		err = tx.QueryRowContext(
			ctx,
			`SELECT agent_id, claim_status FROM node_claims WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
			taskID, nodeID, workspaceID,
		).Scan(&currentAgentID, &currentClaimStatus)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO node_claims(task_id, node_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
					taskID, nodeID, workspaceID, agentID, model.TaskClaimStatusClaimed, strings.TrimSpace(input.Summary), now, now,
				); err != nil {
					return fmt.Errorf("insert node claim: %w", err)
				}
			} else {
				return fmt.Errorf("query node claim: %w", err)
			}
		} else {
			if currentClaimStatus == model.TaskClaimStatusClaimed && currentAgentID != agentID {
				return ErrNodeClaimConflict
			}
			if _, err := tx.ExecContext(
				ctx,
				`UPDATE node_claims
				 SET agent_id = ?, claim_status = ?, summary = ?, released_at = NULL, updated_at = ?, claimed_at = ?
				 WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
				agentID, model.TaskClaimStatusClaimed, strings.TrimSpace(input.Summary), now, now, taskID, nodeID, workspaceID,
			); err != nil {
				return fmt.Errorf("update node claim: %w", err)
			}
		}

		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TaskID:    taskID,
			NodeID:    nodeID,
			NewStatus: model.NodeStatusRunning,
			Reason:    "node_claimed",
			ActorID:   agentID,
		}); err != nil {
			return fmt.Errorf("update dag_nodes to running: %w", err)
		}
		if err := s.incrementNodeAttemptTx(ctx, tx, taskID, nodeID); err != nil {
			return fmt.Errorf("increment node attempt: %w", err)
		}

		var taskStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&taskStatus); err == nil && taskStatus == model.TaskStatusPending {
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?`, model.TaskStatusRunning, now, taskID); err != nil {
				return fmt.Errorf("update task to running: %w", err)
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace: %w", err)
		}

		runtimePayload, err := attachAgentNodePromptContextEnvelope(map[string]any{
			"task_id":           taskID,
			"node_id":           nodeID,
			"agent_id":          agentID,
			"status":            "CLAIMED",
			"node_claim_status": "CLAIMED",
			"node_status_after": model.NodeStatusRunning,
			"summary":           strings.TrimSpace(input.Summary),
			"reason":            "",
		}, input.PromptContextEnvelope, "agent.node.claim", workspaceID, taskID, nodeID, agentID, model.NodeStatusRunning)
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendNodeLifecycleRuntimeEventWithAuthorityTx(ctx, tx, authority, workspaceID, TaskNodeLifecycleRuntimeEventInput{
			TaskID:    taskID,
			NodeID:    nodeID,
			AgentID:   agentID,
			EventType: "node.claimed",
			Payload:   runtimePayload,
		}, now)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit tx: %w", err)
	}

	return runtimeEvent, nil
}

func (s *Store) ReleaseNodeClaimWithEvent(ctx context.Context, input NodeReleaseInput) (RuntimeEventRecord, error) {
	return s.releaseNodeClaimWithEvent(ctx, input)
}

func (s *Store) ReleaseNodeClaim(ctx context.Context, input NodeReleaseInput) error {
	_, err := s.releaseNodeClaimWithEvent(ctx, input)
	return err
}

func (s *Store) releaseNodeClaimWithEvent(ctx context.Context, input NodeReleaseInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return RuntimeEventRecord{}, errors.New("task_id is required")
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID == "" {
		return RuntimeEventRecord{}, errors.New("node_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin node release tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var runtimeEvent RuntimeEventRecord

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var currentAgentID, currentStatus string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT agent_id, claim_status FROM node_claims WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
			taskID, nodeID, workspaceID,
		).Scan(&currentAgentID, &currentStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // Nothing to release
			}
			return fmt.Errorf("query node claim: %w", err)
		}

		if currentStatus != model.TaskClaimStatusClaimed {
			return nil
		}

		if currentAgentID != agentID {
			return ErrNodeClaimConflict
		}

		var nodeStatus string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`,
			taskID,
			nodeID,
		).Scan(&nodeStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNodeNotFound
			}
			return fmt.Errorf("query node status for release: %w", err)
		}

		summary := "Released"
		if strings.TrimSpace(input.Reason) != "" {
			summary = "Released: " + strings.TrimSpace(input.Reason)
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE node_claims
			 SET claim_status = ?, summary = ?, released_at = ?, updated_at = ?
			 WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
			model.TaskClaimStatusReleased, summary, now, now, taskID, nodeID, workspaceID,
		); err != nil {
			return fmt.Errorf("update node claim: %w", err)
		}

		nodeStatusAfter := nodeStatus
		if nodeStatus == model.NodeStatusRunning || nodeStatus == model.NodeStatusPending {
			nextStatus := model.NodeStatusPending
			depsResolved, err := s.nodeDependenciesResolvedTx(ctx, tx, taskID, nodeID)
			if err != nil {
				return fmt.Errorf("check node dependencies for release: %w", err)
			}
			if !depsResolved {
				nextStatus = model.NodeStatusBlocked
			}
			nodeStatusAfter = nextStatus
			if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
				TaskID:    taskID,
				NodeID:    nodeID,
				NewStatus: nextStatus,
				Reason:    strings.TrimSpace(input.Reason),
				ActorID:   agentID,
			}); err != nil {
				return fmt.Errorf("update dag_nodes on release: %w", err)
			}
		}

		if _, err := s.updateTaskStatusFromNodesTx(ctx, tx, taskID, agentID, "node_claim_released", now); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace: %w", err)
		}

		runtimePayload, err := attachAgentNodePromptContextEnvelope(map[string]any{
			"task_id":           taskID,
			"node_id":           nodeID,
			"agent_id":          agentID,
			"status":            "RELEASED",
			"node_claim_status": "RELEASED",
			"node_status_after": nodeStatusAfter,
			"summary":           "",
			"reason":            strings.TrimSpace(input.Reason),
		}, input.PromptContextEnvelope, "agent.node.release", workspaceID, taskID, nodeID, agentID, nodeStatusAfter)
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendNodeLifecycleRuntimeEventWithAuthorityTx(ctx, tx, authority, workspaceID, TaskNodeLifecycleRuntimeEventInput{
			TaskID:    taskID,
			NodeID:    nodeID,
			AgentID:   agentID,
			EventType: "node.released",
			Payload:   runtimePayload,
		}, now)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit node release tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) CompleteNodeClaimWithEvent(ctx context.Context, input NodeCompleteInput) (RuntimeEventRecord, error) {
	return s.completeNodeClaimWithEvent(ctx, input)
}

func (s *Store) CompleteNodeClaim(ctx context.Context, input NodeCompleteInput) error {
	_, err := s.completeNodeClaimWithEvent(ctx, input)
	return err
}

func (s *Store) completeNodeClaimWithEvent(ctx context.Context, input NodeCompleteInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return RuntimeEventRecord{}, errors.New("task_id is required")
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID == "" {
		return RuntimeEventRecord{}, errors.New("node_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin node complete tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var runtimeEvent RuntimeEventRecord

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var currentAgentID, currentStatus string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT agent_id, claim_status FROM node_claims WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
			taskID, nodeID, workspaceID,
		).Scan(&currentAgentID, &currentStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("node is not claimed in this workspace")
			}
			return fmt.Errorf("query node claim: %w", err)
		}

		if currentAgentID != agentID {
			return ErrNodeClaimConflict
		}
		if currentStatus != model.TaskClaimStatusClaimed {
			return fmt.Errorf("node claim is not active: %s", currentStatus)
		}

		var nodeStatus string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT status FROM dag_nodes WHERE task_id = ? AND node_id = ?`,
			taskID,
			nodeID,
		).Scan(&nodeStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNodeNotFound
			}
			return fmt.Errorf("query node status for completion: %w", err)
		}
		if nodeStatus != model.NodeStatusRunning {
			return fmt.Errorf("node %s/%s is %s, expected RUNNING", taskID, nodeID, nodeStatus)
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE node_claims
			 SET claim_status = ?, summary = ?, updated_at = ?
			 WHERE task_id = ? AND node_id = ? AND workspace_id = ?`,
			model.TaskClaimStatusCompleted, strings.TrimSpace(input.Summary), now, taskID, nodeID, workspaceID,
		); err != nil {
			return fmt.Errorf("update node claim: %w", err)
		}

		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TaskID:    taskID,
			NodeID:    nodeID,
			NewStatus: model.NodeStatusResolved,
			Reason:    "node_claim_completed",
			ActorID:   agentID,
		}); err != nil {
			return fmt.Errorf("update dag_nodes to resolved: %w", err)
		}

		rows, err := tx.QueryContext(ctx, `SELECT node_id FROM node_dependencies WHERE task_id = ? AND depends_on_node_id = ?`, taskID, nodeID)
		if err != nil {
			return fmt.Errorf("query dependencies: %w", err)
		}

		var dependentNodes []string
		for rows.Next() {
			var dNode string
			if err := rows.Scan(&dNode); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan dependency: %w", err)
			}
			dependentNodes = append(dependentNodes, dNode)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate dependencies: %w", err)
		}
		_ = rows.Close()

		for _, dNode := range dependentNodes {
			var unresolvedCount int
			err := tx.QueryRowContext(
				ctx,
				`SELECT COUNT(1) FROM node_dependencies nd
				 JOIN dag_nodes dn ON nd.task_id = dn.task_id AND nd.depends_on_node_id = dn.node_id
				 WHERE nd.task_id = ? AND nd.node_id = ? AND dn.status != ?`,
				taskID, dNode, model.NodeStatusResolved,
			).Scan(&unresolvedCount)
			if err != nil {
				return fmt.Errorf("query unresolved count for %s: %w", dNode, err)
			}

			if unresolvedCount == 0 {
				if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
					TaskID:    taskID,
					NodeID:    dNode,
					NewStatus: model.NodeStatusPending,
					Reason:    "dependencies_resolved",
					ActorID:   agentID,
				}); err != nil && !errors.Is(err, orchestrator.ErrInvalidNodeStatusTransition) {
					return fmt.Errorf("unblock node %s: %w", dNode, err)
				}
			}
		}

		var unresolvedTaskNodeCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM dag_nodes WHERE task_id = ? AND status != ?`, taskID, model.NodeStatusResolved).Scan(&unresolvedTaskNodeCount); err != nil {
			return fmt.Errorf("check task completion: %w", err)
		}

		// W2.1 (P04/P05, H1): the task-status terminal write now lives SOLELY in
		// updateTaskStatusFromNodesTx, which gates RESOLVED through evaluateTerminalAdmissionTx. The
		// historical direct "auto-complete task" UPDATE here was removed so a Reject inside the
		// aggregation cannot be undone by a redundant write one line later. The completer-scoped claim
		// sync (CA-09) runs only if the aggregation actually resolved the task.
		nodeAggregateStatus, aggErr := s.updateTaskStatusFromNodesTx(ctx, tx, taskID, agentID, "node_claim_completed", now)
		if aggErr != nil {
			return aggErr
		}
		if unresolvedTaskNodeCount == 0 && nodeAggregateStatus == model.TaskStatusResolved {
			// CA-09: scope the task-claim completion to the node completer's own claim.
			// Without the agent_id predicate, completing the last node force-completed
			// whichever agent held the task-level claim, driving a foreign owner's claim
			// terminal without passing the claim transition/conflict ladder (so that
			// owner's later Release/Complete failed with a stale-transition error). The
			// task still resolves; a foreign owner's claim is left for that owner to
			// release rather than silently mutated here.
			if _, err := tx.ExecContext(ctx, `UPDATE task_claims SET claim_status = ?, updated_at = ? WHERE task_id = ? AND agent_id = ? AND claim_status != ?`, model.TaskClaimStatusCompleted, now, taskID, agentID, model.TaskClaimStatusCompleted); err != nil {
				return fmt.Errorf("sync task claim status on node completion: %w", err)
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace: %w", err)
		}

		runtimePayload, err := attachAgentNodePromptContextEnvelope(map[string]any{
			"task_id":           taskID,
			"node_id":           nodeID,
			"agent_id":          agentID,
			"status":            "COMPLETED",
			"node_claim_status": "COMPLETED",
			"node_status_after": model.NodeStatusResolved,
			"summary":           strings.TrimSpace(input.Summary),
			"reason":            "",
		}, input.PromptContextEnvelope, "agent.node.complete", workspaceID, taskID, nodeID, agentID, model.NodeStatusResolved)
		if err != nil {
			return err
		}
		runtimeEvent, err = s.appendNodeLifecycleRuntimeEventWithAuthorityTx(ctx, tx, authority, workspaceID, TaskNodeLifecycleRuntimeEventInput{
			TaskID:    taskID,
			NodeID:    nodeID,
			AgentID:   agentID,
			EventType: "node.completed",
			Payload:   runtimePayload,
		}, now)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit node complete tx: %w", err)
	}
	return runtimeEvent, nil
}

func (s *Store) nodeDependenciesResolvedTx(ctx context.Context, tx *sql.Tx, taskID, nodeID string) (bool, error) {
	var unresolvedCount int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COUNT(1)
		 FROM node_dependencies nd
		 JOIN dag_nodes dn
		   ON dn.task_id = nd.task_id
		  AND dn.node_id = nd.depends_on_node_id
		 WHERE nd.task_id = ? AND nd.node_id = ? AND dn.status != ?`,
		taskID,
		nodeID,
		model.NodeStatusResolved,
	).Scan(&unresolvedCount); err != nil {
		return false, err
	}
	return unresolvedCount == 0, nil
}

type WorkspaceNodeFilter struct {
	WorkspaceID string
	TaskID      string
	Status      string
	Limit       int
}

type WorkspaceNodeRecord struct {
	TaskID       string  `json:"task_id"`
	NodeID       string  `json:"node_id"`
	NodeType     string  `json:"node_type"`
	Status       string  `json:"status"`
	ClaimAgentID *string `json:"claim_agent_id"`
	ClaimStatus  *string `json:"claim_status"`
	ClaimSummary *string `json:"claim_summary"`
	UpdatedAt    string  `json:"updated_at"`
}

func (s *Store) ListWorkspaceNodes(ctx context.Context, filter WorkspaceNodeFilter) ([]WorkspaceNodeRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 500 {
		limit = 500
	}

	// Make sure the workspace exists
	var dummy int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrWorkspaceNotFound
		}
		return nil, fmt.Errorf("check workspace: %w", err)
	}

	clauses := []string{"wt.workspace_id = ?"}
	args := []any{workspaceID}

	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		clauses = append(clauses, "dn.task_id = ?")
		args = append(args, taskID)
	}

	if status := strings.TrimSpace(filter.Status); status != "" {
		clauses = append(clauses, "dn.status = ?")
		args = append(args, status)
	}

	query := `
SELECT
    dn.task_id, dn.node_id, dn.node_type, dn.status,
    nc.agent_id, nc.claim_status, nc.summary,
    dn.updated_at
FROM dag_nodes dn
JOIN workspace_tasks wt ON wt.task_id = dn.task_id
LEFT JOIN node_claims nc ON nc.task_id = dn.task_id AND nc.node_id = dn.node_id AND nc.workspace_id = wt.workspace_id
WHERE ` + strings.Join(clauses, " AND ") + `
ORDER BY dn.updated_at DESC
LIMIT ?`

	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query workspace nodes: %w", err)
	}
	defer rows.Close()

	var out []WorkspaceNodeRecord
	for rows.Next() {
		var r WorkspaceNodeRecord
		var agentID, claimStatus, summary sql.NullString
		if err := rows.Scan(
			&r.TaskID, &r.NodeID, &r.NodeType, &r.Status,
			&agentID, &claimStatus, &summary,
			&r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan node row: %w", err)
		}
		if agentID.Valid {
			r.ClaimAgentID = &agentID.String
		}
		if claimStatus.Valid {
			r.ClaimStatus = &claimStatus.String
		}
		if summary.Valid {
			r.ClaimSummary = &summary.String
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate node rows: %w", err)
	}

	return out, nil
}
