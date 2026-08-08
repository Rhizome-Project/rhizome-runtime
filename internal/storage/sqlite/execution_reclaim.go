package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// ReclaimExecutableNodesAfterRestart resets locally-owned RUNNING execution
// nodes back to PENDING so a restarted single-host daemon can safely pick them
// up again under the current workspace authority fence.
func (s *Store) ReclaimExecutableNodesAfterRestart(ctx context.Context, actorID string) ([]ExecutableNode, error) {
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("ensure local authority node for execution reclaim: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin execution reclaim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	type reclaimCandidate struct {
		ExecutableNode
		ExpectedLeaseToken string
		ExpectedTerm       int64
	}

	rows, err := tx.QueryContext(
		ctx,
		`WITH task_workspace_scope AS (
		   SELECT wt.task_id,
		          MIN(wt.workspace_id) AS workspace_id,
		          COUNT(1) AS workspace_count
		     FROM workspace_tasks wt
		    GROUP BY wt.task_id
		 )
		 SELECT tw.workspace_id, wa.lease_token, wa.term,
		        n.task_id, n.node_id, n.node_type, n.status, n.attempt_count, t.owner_user_id, t.priority
		 FROM dag_nodes n
		 JOIN tasks t ON t.task_id = n.task_id
		 JOIN task_workspace_scope tw ON tw.task_id = n.task_id
		 JOIN workspace_authority wa ON wa.workspace_id = tw.workspace_id AND wa.scope = ?
		 WHERE t.task_kind = ?
		   AND tw.workspace_count = 1
		   AND wa.holder_authority_node_id = ?
		   AND TRIM(COALESCE(wa.lease_token, '')) != ''
		   AND wa.term > 0
		   AND t.status IN (?, ?)
		   AND n.status = ?
		 ORDER BY n.created_at, n.node_id`,
		authorityScopeWorkspace,
		model.TaskKindExecution,
		localNode.AuthorityNodeID,
		model.TaskStatusPending,
		model.TaskStatusRunning,
		model.NodeStatusRunning,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution reclaim candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]reclaimCandidate, 0, 8)
	for rows.Next() {
		var row reclaimCandidate
		if err := rows.Scan(
			&row.WorkspaceID,
			&row.ExpectedLeaseToken,
			&row.ExpectedTerm,
			&row.TaskID,
			&row.NodeID,
			&row.NodeType,
			&row.Status,
			&row.AttemptCount,
			&row.OwnerUserID,
			&row.Priority,
		); err != nil {
			return nil, fmt.Errorf("scan execution reclaim candidate: %w", err)
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution reclaim candidates: %w", err)
	}

	reclaimed := make([]ExecutableNode, 0, len(candidates))
	checkedWorkspaces := make(map[string]struct{}, len(candidates))
	for _, row := range candidates {
		if _, seen := checkedWorkspaces[row.WorkspaceID]; !seen {
			fenceInput := WorkspaceAuthorityFenceInput{
				WorkspaceID:                   row.WorkspaceID,
				Scope:                         authorityScopeWorkspace,
				ExpectedHolderAuthorityNodeID: localNode.AuthorityNodeID,
				ExpectedLeaseToken:            row.ExpectedLeaseToken,
				ExpectedTerm:                  row.ExpectedTerm,
				ReferenceAt:                   now,
			}
			if _, err := s.checkWorkspaceAuthorityFenceTx(ctx, tx, fenceInput); err != nil {
				_ = tx.Rollback()
				return nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
			}
			checkedWorkspaces[row.WorkspaceID] = struct{}{}
		}

		if err := s.setNodeStatusTx(ctx, tx, NodeStatusUpdateInput{
			TaskID:    row.TaskID,
			NodeID:    row.NodeID,
			NewStatus: model.NodeStatusPending,
			Reason:    "executor_reclaimed_after_restart",
			ActorID:   strings.TrimSpace(actorID),
		}); err != nil {
			return nil, fmt.Errorf("reclaim executable node %s/%s: %w", row.TaskID, row.NodeID, err)
		}
		if _, err := s.updateTaskStatusFromNodesTx(ctx, tx, row.TaskID, strings.TrimSpace(actorID), "node_execution_reclaimed_after_restart", now); err != nil {
			return nil, fmt.Errorf("recalculate task status after reclaim %s/%s: %w", row.TaskID, row.NodeID, err)
		}
		row.Status = model.NodeStatusPending
		reclaimed = append(reclaimed, row.ExecutableNode)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit execution reclaim tx: %w", err)
	}
	return reclaimed, nil
}
