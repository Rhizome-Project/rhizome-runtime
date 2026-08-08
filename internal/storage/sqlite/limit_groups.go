package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ── Limit Group types ────────────────────────────────────────────────

type LimitGroupRecord struct {
	GroupID          string   `json:"group_id"`
	WorkspaceID      string   `json:"workspace_id"`
	Title            string   `json:"title"`
	OwnerName        string   `json:"owner_name"`
	SubscriptionTier string   `json:"subscription_tier"`
	DailyLimit       int      `json:"daily_limit"`
	WeeklyLimit      int      `json:"weekly_limit"`
	DailyRemaining   int      `json:"daily_remaining"`
	WeeklyRemaining  int      `json:"weekly_remaining"`
	LastReportedAt   string   `json:"last_reported_at,omitempty"`
	CreatedAt        string   `json:"created_at"`
	UpdatedAt        string   `json:"updated_at"`
	Agents           []string `json:"agents"`
}

type LimitGroupCreateInput struct {
	GroupID          string
	WorkspaceID      string
	Title            string
	OwnerName        string
	SubscriptionTier string
	DailyLimit       int
	WeeklyLimit      int
	ActorID          string
	ActorType        string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type LimitGroupUpdateInput struct {
	WorkspaceID      string
	GroupID          string
	Title            string
	OwnerName        string
	SubscriptionTier string
	DailyLimit       *int
	WeeklyLimit      *int
	AgentIDs         []string // full replacement of agent membership
	ActorID          string
	ActorType        string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type LimitGroupDeleteInput struct {
	WorkspaceID string
	GroupID     string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type LimitReportInput struct {
	GroupID         string
	AgentID         string
	DailyRemaining  int
	WeeklyRemaining int
}

type LimitSnapshotRecord struct {
	SnapshotID      string `json:"snapshot_id"`
	GroupID         string `json:"group_id"`
	AgentID         string `json:"agent_id"`
	DailyRemaining  int    `json:"daily_remaining"`
	WeeklyRemaining int    `json:"weekly_remaining"`
	ReportedAt      string `json:"reported_at"`
}

// ── Limit Group CRUD ─────────────────────────────────────────────────

func (s *Store) CreateLimitGroup(ctx context.Context, input LimitGroupCreateInput) error {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return errors.New("group_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return errors.New("title is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin limit group create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.createLimitGroupTx(ctx, tx, input, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit limit group create: %w", err)
	}
	return nil
}

func (s *Store) CreateLimitGroupWithEvent(ctx context.Context, input LimitGroupCreateInput) (LimitGroupRecord, RuntimeEventRecord, error) {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("group_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin limit group create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.createLimitGroupTx(ctx, tx, input, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "limit group create"); err != nil {
			return err
		}
		payload, err := attachLimitGroupPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"group_id":           groupID,
			"actor_id":           actorID,
			"title":              title,
			"owner_name":         strings.TrimSpace(input.OwnerName),
			"subscription_tier":  strings.TrimSpace(input.SubscriptionTier),
			"daily_limit":        input.DailyLimit,
			"weekly_limit":       input.WeeklyLimit,
			"agent_ids":          []string{},
			"agent_ids_count":    0,
			"entity_type":        "limit_group",
			"entity_id":          groupID,
			"summary":            "Limit group created: " + title,
			"mutation_operation": "create",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "limits.group.create"), map[string]string{
			"workspace_id":   workspaceID,
			"group_id":       groupID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "limits.group.created",
			EntityType:  "limit_group",
			EntityID:    groupID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit limit group create: %w", err)
	}
	group, err := s.GetLimitGroup(ctx, workspaceID, groupID)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, err
	}
	return *group, event, nil
}

func (s *Store) createLimitGroupTx(ctx context.Context, tx *sql.Tx, input LimitGroupCreateInput, now string) error {
	groupID := strings.TrimSpace(input.GroupID)
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	title := strings.TrimSpace(input.Title)
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO limit_groups (group_id, workspace_id, title, owner_name, subscription_tier,
		  daily_limit, weekly_limit, daily_remaining, weekly_remaining, last_reported_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)`,
		groupID, workspaceID, title,
		strings.TrimSpace(input.OwnerName),
		strings.TrimSpace(input.SubscriptionTier),
		input.DailyLimit, input.WeeklyLimit,
		input.DailyLimit, input.WeeklyLimit,
		now, now,
	); err != nil {
		return fmt.Errorf("create limit group: %w", err)
	}
	return nil
}

func (s *Store) ListLimitGroups(ctx context.Context, workspaceID string) ([]LimitGroupRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, workspace_id, title, owner_name, subscription_tier,
		        daily_limit, weekly_limit, daily_remaining, weekly_remaining,
		        last_reported_at, created_at, updated_at
		 FROM limit_groups
		 WHERE workspace_id = ?
		 ORDER BY created_at DESC`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list limit groups: %w", err)
	}
	defer rows.Close()

	var groups []LimitGroupRecord
	for rows.Next() {
		var g LimitGroupRecord
		if err := rows.Scan(&g.GroupID, &g.WorkspaceID, &g.Title, &g.OwnerName, &g.SubscriptionTier,
			&g.DailyLimit, &g.WeeklyLimit, &g.DailyRemaining, &g.WeeklyRemaining,
			&g.LastReportedAt, &g.CreatedAt, &g.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan limit group: %w", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate limit groups: %w", err)
	}

	// Load agents for each group
	for i := range groups {
		agents, err := s.listLimitGroupAgents(ctx, groups[i].GroupID)
		if err != nil {
			return nil, err
		}
		groups[i].Agents = agents
	}
	return groups, nil
}

func (s *Store) GetLimitGroup(ctx context.Context, workspaceID, groupID string) (*LimitGroupRecord, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("group_id is required")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT group_id, workspace_id, title, owner_name, subscription_tier,
		        daily_limit, weekly_limit, daily_remaining, weekly_remaining,
		        last_reported_at, created_at, updated_at
		 FROM limit_groups
		 WHERE group_id = ? AND workspace_id = ?`,
		groupID, strings.TrimSpace(workspaceID),
	)
	var g LimitGroupRecord
	if err := row.Scan(&g.GroupID, &g.WorkspaceID, &g.Title, &g.OwnerName, &g.SubscriptionTier,
		&g.DailyLimit, &g.WeeklyLimit, &g.DailyRemaining, &g.WeeklyRemaining,
		&g.LastReportedAt, &g.CreatedAt, &g.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("limit group not found: %s", groupID)
		}
		return nil, fmt.Errorf("get limit group: %w", err)
	}
	agents, err := s.listLimitGroupAgents(ctx, g.GroupID)
	if err != nil {
		return nil, err
	}
	g.Agents = agents
	return &g, nil
}

func (s *Store) UpdateLimitGroup(ctx context.Context, input LimitGroupUpdateInput) error {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return errors.New("group_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin limit group update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.updateLimitGroupTx(ctx, tx, input, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit limit group update: %w", err)
	}
	return nil
}

func (s *Store) UpdateLimitGroupWithEvent(ctx context.Context, input LimitGroupUpdateInput) (LimitGroupRecord, RuntimeEventRecord, error) {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("group_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return LimitGroupRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin limit group update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	agentIDs := []string(nil)
	if input.AgentIDs != nil {
		agentIDs = normalizeLimitGroupAgentIDs(input.AgentIDs)
	}
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.updateLimitGroupTx(ctx, tx, input, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "limit group update"); err != nil {
			return err
		}
		payload, err := attachLimitGroupPromptContextEnvelope(map[string]any{
			"workspace_id":                 workspaceID,
			"group_id":                     groupID,
			"actor_id":                     actorID,
			"title":                        strings.TrimSpace(input.Title),
			"owner_name":                   strings.TrimSpace(input.OwnerName),
			"subscription_tier":            strings.TrimSpace(input.SubscriptionTier),
			"agent_ids":                    agentIDs,
			"agent_ids_count":              len(agentIDs),
			"agent_membership_replacement": input.AgentIDs != nil,
			"entity_type":                  "limit_group",
			"entity_id":                    groupID,
			"summary":                      "Limit group updated: " + groupID,
			"mutation_operation":           "update",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "limits.group.update"), map[string]string{
			"workspace_id":   workspaceID,
			"group_id":       groupID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		if input.DailyLimit != nil {
			payload["daily_limit"] = *input.DailyLimit
		}
		if input.WeeklyLimit != nil {
			payload["weekly_limit"] = *input.WeeklyLimit
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "limits.group.updated",
			EntityType:  "limit_group",
			EntityID:    groupID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit limit group update: %w", err)
	}
	group, err := s.GetLimitGroup(ctx, workspaceID, groupID)
	if err != nil {
		return LimitGroupRecord{}, RuntimeEventRecord{}, err
	}
	return *group, event, nil
}

func (s *Store) updateLimitGroupTx(ctx context.Context, tx *sql.Tx, input LimitGroupUpdateInput, now string) error {
	groupID := strings.TrimSpace(input.GroupID)
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	var existing int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM limit_groups WHERE group_id = ? AND workspace_id = ?`,
		groupID,
		workspaceID,
	).Scan(&existing); err != nil {
		return fmt.Errorf("check limit group before update: %w", err)
	}
	if existing == 0 {
		return fmt.Errorf("limit group not found: %s", groupID)
	}

	sets := []string{}
	args := []any{}

	if t := strings.TrimSpace(input.Title); t != "" {
		sets = append(sets, "title = ?")
		args = append(args, t)
	}
	if o := strings.TrimSpace(input.OwnerName); o != "" {
		sets = append(sets, "owner_name = ?")
		args = append(args, o)
	}
	if st := strings.TrimSpace(input.SubscriptionTier); st != "" {
		sets = append(sets, "subscription_tier = ?")
		args = append(args, st)
	}
	if input.DailyLimit != nil {
		sets = append(sets, "daily_limit = ?")
		args = append(args, *input.DailyLimit)
	}
	if input.WeeklyLimit != nil {
		sets = append(sets, "weekly_limit = ?")
		args = append(args, *input.WeeklyLimit)
	}

	if len(sets) > 0 {
		sets = append(sets, "updated_at = ?")
		args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
		args = append(args, groupID, workspaceID)

		query := "UPDATE limit_groups SET " + strings.Join(sets, ", ") + " WHERE group_id = ? AND workspace_id = ?"
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("update limit group: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("limit group not found: %s", groupID)
		}
	}

	// Replace agent membership if provided
	if input.AgentIDs != nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM limit_group_agents WHERE group_id = ? AND workspace_id = ?`, groupID, workspaceID); err != nil {
			return fmt.Errorf("clear limit group agents: %w", err)
		}
		for _, agentID := range normalizeLimitGroupAgentIDs(input.AgentIDs) {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO limit_group_agents (group_id, workspace_id, agent_id, joined_at) VALUES (?, ?, ?, ?)`,
				groupID, workspaceID, agentID, now,
			); err != nil {
				return fmt.Errorf("add agent %s to limit group: %w", agentID, err)
			}
		}
		if len(sets) == 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE limit_groups SET updated_at = ? WHERE group_id = ? AND workspace_id = ?`,
				now, groupID, workspaceID,
			); err != nil {
				return fmt.Errorf("touch limit group after membership update: %w", err)
			}
		}
	}
	return nil
}

func (s *Store) DeleteLimitGroup(ctx context.Context, workspaceID, groupID string) error {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return errors.New("group_id is required")
	}
	// CASCADE will handle limit_group_agents and limit_snapshots
	res, err := s.writeDB.ExecContext(ctx,
		`DELETE FROM limit_groups WHERE group_id = ? AND workspace_id = ?`,
		groupID, strings.TrimSpace(workspaceID),
	)
	if err != nil {
		return fmt.Errorf("delete limit group: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("limit group not found: %s", groupID)
	}
	return nil
}

func (s *Store) DeleteLimitGroupWithEvent(ctx context.Context, input LimitGroupDeleteInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return RuntimeEventRecord{}, errors.New("group_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin limit group delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		agents, err := s.listLimitGroupAgentsTx(ctx, tx, workspaceID, groupID)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`DELETE FROM limit_groups WHERE group_id = ? AND workspace_id = ?`,
			groupID, workspaceID,
		)
		if err != nil {
			return fmt.Errorf("delete limit group: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("limit group not found: %s", groupID)
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "limit group delete"); err != nil {
			return err
		}
		payload, err := attachLimitGroupPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"group_id":           groupID,
			"actor_id":           actorID,
			"agent_ids":          agents,
			"agent_ids_count":    len(agents),
			"entity_type":        "limit_group",
			"entity_id":          groupID,
			"summary":            "Limit group deleted: " + groupID,
			"mutation_operation": "delete",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "limits.group.delete"), map[string]string{
			"workspace_id":   workspaceID,
			"group_id":       groupID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "limits.group.deleted",
			EntityType:  "limit_group",
			EntityID:    groupID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit limit group delete: %w", err)
	}
	return event, nil
}

// ── Limit Reporting ──────────────────────────────────────────────────

func (s *Store) ReportLimits(ctx context.Context, input LimitReportInput) error {
	groupID := strings.TrimSpace(input.GroupID)
	if groupID == "" {
		return errors.New("group_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin limit report tx: %w", err)
	}
	var workspaceID string
	if err := tx.QueryRowContext(ctx, `SELECT workspace_id FROM limit_groups WHERE group_id = ?`, groupID).Scan(&workspaceID); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("limit group not found: %s", groupID)
		}
		return fmt.Errorf("query limit group workspace: %w", err)
	}
	var membershipCount int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM limit_group_agents WHERE group_id = ? AND workspace_id = ? AND agent_id = ?`,
		groupID,
		workspaceID,
		agentID,
	).Scan(&membershipCount); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("check limit group membership: %w", err)
	}
	if membershipCount == 0 {
		_ = tx.Rollback()
		return fmt.Errorf("agent %s is not assigned to limit group %s", agentID, groupID)
	}

	// Update group remaining values
	res, err := tx.ExecContext(ctx,
		`UPDATE limit_groups
		 SET daily_remaining = ?, weekly_remaining = ?, last_reported_at = ?, updated_at = ?
		 WHERE group_id = ?`,
		input.DailyRemaining, input.WeeklyRemaining, now, now, groupID,
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("update limit group remaining: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		_ = tx.Rollback()
		return fmt.Errorf("limit group not found: %s", groupID)
	}

	// Insert snapshot for audit trail
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO limit_snapshots (snapshot_id, group_id, workspace_id, agent_id, daily_remaining, weekly_remaining, reported_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		nextID("lsnap"), groupID, workspaceID, agentID, input.DailyRemaining, input.WeeklyRemaining, now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert limit snapshot: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit limit report: %w", err)
	}
	return nil
}

// GetAgentLimitGroup returns the limit group an agent belongs to (if any).
func (s *Store) GetAgentLimitGroup(ctx context.Context, workspaceID, agentID string) (*LimitGroupRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, errors.New("agent_id is required")
	}

	row := s.db.QueryRowContext(ctx,
		`SELECT g.group_id, g.workspace_id, g.title, g.owner_name, g.subscription_tier,
		        g.daily_limit, g.weekly_limit, g.daily_remaining, g.weekly_remaining,
		        g.last_reported_at, g.created_at, g.updated_at
		 FROM limit_groups g
		 JOIN limit_group_agents ga ON ga.group_id = g.group_id AND ga.workspace_id = g.workspace_id
		 WHERE ga.workspace_id = ? AND ga.agent_id = ?`,
		workspaceID,
		agentID,
	)
	var g LimitGroupRecord
	if err := row.Scan(&g.GroupID, &g.WorkspaceID, &g.Title, &g.OwnerName, &g.SubscriptionTier,
		&g.DailyLimit, &g.WeeklyLimit, &g.DailyRemaining, &g.WeeklyRemaining,
		&g.LastReportedAt, &g.CreatedAt, &g.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // agent not in any group
		}
		return nil, fmt.Errorf("get agent limit group: %w", err)
	}
	agents, err := s.listLimitGroupAgents(ctx, g.GroupID)
	if err != nil {
		return nil, err
	}
	g.Agents = agents
	return &g, nil
}

func (s *Store) ListLimitSnapshots(ctx context.Context, groupID string, limit int) ([]LimitSnapshotRecord, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("group_id is required")
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT snapshot_id, group_id, agent_id, daily_remaining, weekly_remaining, reported_at
		 FROM limit_snapshots
		 WHERE group_id = ?
		 ORDER BY reported_at DESC
		 LIMIT ?`,
		groupID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list limit snapshots: %w", err)
	}
	defer rows.Close()

	var snaps []LimitSnapshotRecord
	for rows.Next() {
		var s LimitSnapshotRecord
		if err := rows.Scan(&s.SnapshotID, &s.GroupID, &s.AgentID, &s.DailyRemaining, &s.WeeklyRemaining, &s.ReportedAt); err != nil {
			return nil, fmt.Errorf("scan limit snapshot: %w", err)
		}
		snaps = append(snaps, s)
	}
	return snaps, rows.Err()
}

// ListLimitSnapshotsByRange returns snapshots for a group within the last `days` days, ordered chronologically.
func (s *Store) ListLimitSnapshotsByRange(ctx context.Context, groupID string, days int) ([]LimitSnapshotRecord, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("group_id is required")
	}
	if days <= 0 {
		days = 7
	}
	since := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx,
		`SELECT snapshot_id, group_id, agent_id, daily_remaining, weekly_remaining, reported_at
		 FROM limit_snapshots
		 WHERE group_id = ? AND reported_at >= ?
		 ORDER BY reported_at ASC`,
		groupID, since,
	)
	if err != nil {
		return nil, fmt.Errorf("list limit snapshots by range: %w", err)
	}
	defer rows.Close()

	var snaps []LimitSnapshotRecord
	for rows.Next() {
		var s LimitSnapshotRecord
		if err := rows.Scan(&s.SnapshotID, &s.GroupID, &s.AgentID, &s.DailyRemaining, &s.WeeklyRemaining, &s.ReportedAt); err != nil {
			return nil, fmt.Errorf("scan limit snapshot: %w", err)
		}
		snaps = append(snaps, s)
	}
	return snaps, rows.Err()
}

// ── Helpers ──────────────────────────────────────────────────────────

func (s *Store) listLimitGroupAgents(ctx context.Context, groupID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT agent_id FROM limit_group_agents WHERE group_id = ? ORDER BY agent_id`,
		groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("list limit group agents: %w", err)
	}
	defer rows.Close()

	var agents []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan limit group agent: %w", err)
		}
		agents = append(agents, id)
	}
	if agents == nil {
		agents = []string{}
	}
	return agents, rows.Err()
}

func (s *Store) listLimitGroupAgentsTx(ctx context.Context, tx *sql.Tx, workspaceID, groupID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT agent_id FROM limit_group_agents WHERE workspace_id = ? AND group_id = ? ORDER BY agent_id`,
		workspaceID, groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("list limit group agents: %w", err)
	}
	defer rows.Close()

	var agents []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan limit group agent: %w", err)
		}
		agents = append(agents, id)
	}
	if agents == nil {
		agents = []string{}
	}
	return agents, rows.Err()
}

func normalizeLimitGroupAgentIDs(agentIDs []string) []string {
	if agentIDs == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(agentIDs))
	out := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		out = append(out, agentID)
	}
	sort.Strings(out)
	return out
}

func attachLimitGroupPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachLimitGroupPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"group_id":     strings.TrimSpace(fields["group_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("limit_group.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) touchWorkspaceTx(ctx context.Context, tx *sql.Tx, workspaceID, now, reason string) error {
	res, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID)
	if err != nil {
		return fmt.Errorf("touch workspace after %s: %w", reason, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWorkspaceNotFound
	}
	return nil
}

func singletonLimitGroupID(workspaceID, agentID string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" {
		return "limits-" + agentID
	}
	return "limits-" + workspaceID + "-" + agentID
}

// EnsureAgentLimitGroup creates a singleton limit group for an agent if none exists.
// Idempotent: does nothing if agent already belongs to a group.
func (s *Store) EnsureAgentLimitGroup(ctx context.Context, workspaceID, agentID, displayName string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}

	// Check if agent already in a group
	existing, err := s.GetAgentLimitGroup(ctx, workspaceID, agentID)
	if err != nil {
		return fmt.Errorf("check existing limit group: %w", err)
	}
	if existing != nil {
		return nil // already has a group
	}

	groupID := singletonLimitGroupID(workspaceID, agentID)
	title := strings.TrimSpace(displayName)
	if title == "" {
		title = agentID
	}

	// Create the group (INSERT OR IGNORE to handle race conditions)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.writeDB.ExecContext(ctx,
		`INSERT OR IGNORE INTO limit_groups (group_id, workspace_id, title, owner_name, subscription_tier,
		  daily_limit, weekly_limit, daily_remaining, weekly_remaining, last_reported_at, created_at, updated_at)
		 VALUES (?, ?, ?, '', '', 100, 100, 100, 100, '', ?, ?)`,
		groupID, workspaceID, title, now, now,
	)
	if err != nil {
		return fmt.Errorf("ensure limit group create: %w", err)
	}

	// Add agent to group (INSERT OR IGNORE for idempotency)
	_, err = s.writeDB.ExecContext(ctx,
		`INSERT OR IGNORE INTO limit_group_agents (group_id, workspace_id, agent_id, joined_at) VALUES (?, ?, ?, ?)`,
		groupID, workspaceID, agentID, now,
	)
	if err != nil {
		return fmt.Errorf("ensure limit group agent: %w", err)
	}
	return nil
}

// AssignAgentLimitGroup ensures a specific shared limit group exists for the
// workspace and makes it the agent's canonical membership for that workspace.
func (s *Store) AssignAgentLimitGroup(ctx context.Context, workspaceID, agentID, groupID, title string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent_id is required")
	}
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return errors.New("group_id is required")
	}
	title = strings.TrimSpace(title)
	if title == "" {
		title = groupID
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin assign limit group tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var agentCount int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM agents WHERE workspace_id = ? AND agent_id = ?`,
		workspaceID,
		agentID,
	).Scan(&agentCount); err != nil {
		return fmt.Errorf("check agent before limit group assignment: %w", err)
	}
	if agentCount == 0 {
		return ErrAgentNotFound
	}

	var existingWorkspaceID string
	err = tx.QueryRowContext(ctx,
		`SELECT workspace_id FROM limit_groups WHERE group_id = ?`,
		groupID,
	).Scan(&existingWorkspaceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO limit_groups (group_id, workspace_id, title, owner_name, subscription_tier,
			  daily_limit, weekly_limit, daily_remaining, weekly_remaining, last_reported_at, created_at, updated_at)
			 VALUES (?, ?, ?, '', '', 100, 100, 100, 100, '', ?, ?)`,
			groupID, workspaceID, title, now, now,
		); err != nil {
			return fmt.Errorf("create assigned limit group: %w", err)
		}
	case err != nil:
		return fmt.Errorf("query assigned limit group workspace: %w", err)
	case strings.TrimSpace(existingWorkspaceID) != workspaceID:
		return fmt.Errorf("limit group %q belongs to workspace %q, not %q", groupID, strings.TrimSpace(existingWorkspaceID), workspaceID)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM limit_group_agents WHERE workspace_id = ? AND agent_id = ? AND group_id <> ?`,
		workspaceID,
		agentID,
		groupID,
	); err != nil {
		return fmt.Errorf("clear stale limit group memberships: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO limit_group_agents (group_id, workspace_id, agent_id, joined_at) VALUES (?, ?, ?, ?)`,
		groupID, workspaceID, agentID, now,
	); err != nil {
		return fmt.Errorf("assign agent to limit group: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit assign limit group: %w", err)
	}
	return nil
}

// BootstrapLimitGroups creates singleton limit groups for all agents in a workspace
// that don't already have one. Returns the number of groups created.
func (s *Store) BootstrapLimitGroups(ctx context.Context, workspaceID string) (int, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return 0, errors.New("workspace_id is required")
	}

	// Get all agents that don't have a limit group
	rows, err := s.db.QueryContext(ctx,
		`SELECT a.agent_id, a.display_name
		 FROM agents a
		 WHERE a.workspace_id = ?
		   AND a.agent_id NOT IN (SELECT agent_id FROM limit_group_agents WHERE workspace_id = ?)`,
		workspaceID,
		workspaceID,
	)
	if err != nil {
		return 0, fmt.Errorf("list orphan agents: %w", err)
	}
	defer rows.Close()

	type agentInfo struct {
		id, name string
	}
	var orphans []agentInfo
	for rows.Next() {
		var ai agentInfo
		if err := rows.Scan(&ai.id, &ai.name); err != nil {
			return 0, fmt.Errorf("scan orphan agent: %w", err)
		}
		orphans = append(orphans, ai)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	created := 0
	for _, ai := range orphans {
		if err := s.EnsureAgentLimitGroup(ctx, workspaceID, ai.id, ai.name); err != nil {
			return created, fmt.Errorf("bootstrap limit group for %s: %w", ai.id, err)
		}
		created++
	}
	return created, nil
}
