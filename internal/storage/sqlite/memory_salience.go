package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

type MemoryNodeSalienceRecord struct {
	MemoryID    string  `json:"memory_id"`
	WorkspaceID string  `json:"workspace_id"`
	A_i         float64 `json:"a_i"`
	T_i_star    string  `json:"t_i_star"`
	T_i_acc     string  `json:"t_i_acc"`
	N_i         int     `json:"n_i"`
	Q_i         float64 `json:"q_i"`
	H_i         float64 `json:"h_i"`
	T_hot       string  `json:"t_hot"`
	T_warm      string  `json:"t_warm"`
	T_gc        string  `json:"t_gc"`
	UpdatedAt   string  `json:"updated_at"`
}

type MemoryNodeTouchInput struct {
	WorkspaceID           string
	NodeID                string
	Trusted               bool
	RiskAgent             float64
	SalienceConfig        RMPSalienceConfig
	ActorType             string
	ActorID               string
	PromptContextEnvelope map[string]any
}

type MemoryNodeTouchResult struct {
	Salience     MemoryNodeSalienceRecord `json:"salience"`
	RuntimeEvent RuntimeEventRecord       `json:"runtime_event"`
}

type RMPSalienceConfig struct {
	ThetaHot  float64
	ThetaWarm float64
	ThetaGc   float64
	HMin      float64 // in seconds
	HBar      float64
	HMax      float64
	EtaN      float64
	EtaQ      float64
	RhoQ      float64
	RhoTau    float64
}

const rmpArchivedReasonExpired = "rmp_gc_expired"
const rmpArchivedByPruner = "rmp_pruner"

type rmpPruneCandidate struct {
	MemoryID   string
	OriginKind string
	OriginID   string
}

func DefaultRMPSalienceConfig() RMPSalienceConfig {
	return RMPSalienceConfig{
		ThetaHot:  0.8,
		ThetaWarm: 0.5,
		ThetaGc:   0.1,
		HMin:      3600,       // 1 hour
		HBar:      86400 * 7,  // 1 week
		HMax:      86400 * 90, // 90 days
		EtaN:      0.1,
		EtaQ:      0.3,
		RhoQ:      0.9,
		RhoTau:    0.5,
	}
}

func normalizeRMPSalienceConfig(cfg RMPSalienceConfig) RMPSalienceConfig {
	if cfg.ThetaHot == 0 && cfg.ThetaWarm == 0 && cfg.ThetaGc == 0 &&
		cfg.HMin == 0 && cfg.HBar == 0 && cfg.HMax == 0 &&
		cfg.EtaN == 0 && cfg.EtaQ == 0 && cfg.RhoQ == 0 && cfg.RhoTau == 0 {
		return DefaultRMPSalienceConfig()
	}
	return cfg
}

func rmpComputeThresholdTime(tStar string, h_i, a_i, theta float64) string {
	if a_i <= theta || h_i <= 0 {
		return tStar
	}
	ts, err := time.Parse(time.RFC3339Nano, tStar)
	if err != nil {
		return tStar
	}
	deltaSec := h_i * math.Log2(a_i/theta)
	return ts.Add(time.Duration(deltaSec * float64(time.Second))).UTC().Format(time.RFC3339Nano)
}

func (s *Store) TouchMemoryNodeTrusted(ctx context.Context, workspaceID, memoryID string, riskAgent float64, cfg RMPSalienceConfig) error {
	now := time.Now().UTC()
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := s.touchMemoryNodeTrustedTx(ctx, tx, workspaceID, memoryID, riskAgent, cfg, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) touchMemoryNodeTrustedTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string, riskAgent float64, cfg RMPSalienceConfig, now time.Time) (MemoryNodeSalienceRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryID = strings.TrimSpace(memoryID)
	if workspaceID == "" {
		return MemoryNodeSalienceRecord{}, errors.New("workspace_id is required")
	}
	if memoryID == "" {
		return MemoryNodeSalienceRecord{}, errors.New("memory_id is required")
	}
	cfg = normalizeRMPSalienceConfig(cfg)
	nowStr := now.UTC().Format(time.RFC3339Nano)

	var rec MemoryNodeSalienceRecord
	rec.WorkspaceID = workspaceID
	rec.MemoryID = memoryID
	err := tx.QueryRowContext(ctx, `SELECT a_i, t_i_star, t_i_acc, n_i, q_i, h_i FROM memory_node_salience WHERE memory_id = ? AND workspace_id = ?`, memoryID, workspaceID).Scan(
		&rec.A_i, &rec.T_i_star, &rec.T_i_acc, &rec.N_i, &rec.Q_i, &rec.H_i)

	if err != nil {
		if err == sql.ErrNoRows {
			rec = MemoryNodeSalienceRecord{
				MemoryID:    memoryID,
				WorkspaceID: workspaceID,
				A_i:         1.0,
				T_i_star:    nowStr,
				T_i_acc:     nowStr,
				N_i:         0,
				Q_i:         0.0,
				H_i:         cfg.HBar,
			}
		} else {
			return MemoryNodeSalienceRecord{}, err
		}
	} else {
		// Update based on appendix formula
		tStar, _ := time.Parse(time.RFC3339Nano, rec.T_i_star)
		tAcc, _ := time.Parse(time.RFC3339Nano, rec.T_i_acc)

		deltaT := now.Sub(tAcc).Seconds()
		if deltaT < 0 {
			deltaT = 0
		}

		rec.Q_i = cfg.RhoQ*rec.Q_i + math.Log(1.0+(deltaT/cfg.HBar))
		rec.N_i += 1

		multiplier := math.Pow(1.0+math.Log(float64(1+rec.N_i)), cfg.EtaN) * math.Pow(1.0+rec.Q_i, cfg.EtaQ)
		newH := cfg.HBar * multiplier
		if newH < cfg.HMin {
			newH = cfg.HMin
		}
		if newH > cfg.HMax {
			newH = cfg.HMax
		}
		rec.H_i = newH

		r_i := 1.0 - clampUnitInterval(riskAgent)

		elapsedFromStar := now.Sub(tStar).Seconds()
		if elapsedFromStar < 0 {
			elapsedFromStar = 0
		}

		S_minus := rec.A_i * math.Pow(2.0, -elapsedFromStar/rec.H_i)

		rec.A_i = 1.0 - (1.0-S_minus)*math.Exp(-r_i*cfg.RhoTau)

		newStar := tStar.Add(time.Duration(r_i * elapsedFromStar * float64(time.Second)))
		rec.T_i_star = newStar.UTC().Format(time.RFC3339Nano)
		rec.T_i_acc = nowStr
	}

	tHot := rmpComputeThresholdTime(rec.T_i_star, rec.H_i, rec.A_i, cfg.ThetaHot)
	tWarm := rmpComputeThresholdTime(rec.T_i_star, rec.H_i, rec.A_i, cfg.ThetaWarm)
	tGc := rmpComputeThresholdTime(rec.T_i_star, rec.H_i, rec.A_i, cfg.ThetaGc)
	rec.T_hot = tHot
	rec.T_warm = tWarm
	rec.T_gc = tGc
	rec.UpdatedAt = nowStr

	_, err = tx.ExecContext(ctx, `
		INSERT INTO memory_node_salience (memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			a_i=excluded.a_i,
			t_i_star=excluded.t_i_star,
			t_i_acc=excluded.t_i_acc,
			n_i=excluded.n_i,
			q_i=excluded.q_i,
			h_i=excluded.h_i,
			t_hot=excluded.t_hot,
			t_warm=excluded.t_warm,
			t_gc=excluded.t_gc,
			updated_at=excluded.updated_at
	`, memoryID, workspaceID, rec.A_i, rec.T_i_star, rec.T_i_acc, rec.N_i, rec.Q_i, rec.H_i, tHot, tWarm, tGc, nowStr)

	if err != nil {
		return MemoryNodeSalienceRecord{}, err
	}
	return rec, nil
}

func (s *Store) TouchMemoryNodeUntrusted(ctx context.Context, workspaceID, memoryID string) error {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	// Unsafe access must not refresh the surfaced anchor-state clocks.
	query := `UPDATE memory_node_salience SET updated_at = ? WHERE memory_id = ? AND workspace_id = ?`
	_, err := s.writeDB.ExecContext(ctx, query, nowStr, memoryID, workspaceID)
	return err
}

func (s *Store) touchMemoryNodeUntrustedTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string, now time.Time, requireExisting bool) (MemoryNodeSalienceRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryID = strings.TrimSpace(memoryID)
	if workspaceID == "" {
		return MemoryNodeSalienceRecord{}, errors.New("workspace_id is required")
	}
	if memoryID == "" {
		return MemoryNodeSalienceRecord{}, errors.New("memory_id is required")
	}
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE memory_node_salience SET updated_at = ? WHERE memory_id = ? AND workspace_id = ?`, nowStr, memoryID, workspaceID)
	if err != nil {
		return MemoryNodeSalienceRecord{}, err
	}
	if requireExisting {
		rows, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return MemoryNodeSalienceRecord{}, rowsErr
		}
		if rows == 0 {
			return MemoryNodeSalienceRecord{}, fmt.Errorf("memory node salience not found: %s/%s", workspaceID, memoryID)
		}
	}
	rec, ok, err := s.getMemoryNodeSalienceTx(ctx, tx, workspaceID, memoryID)
	if err != nil {
		return MemoryNodeSalienceRecord{}, err
	}
	if !ok {
		return MemoryNodeSalienceRecord{
			MemoryID:    memoryID,
			WorkspaceID: workspaceID,
			UpdatedAt:   nowStr,
		}, nil
	}
	return rec, nil
}

func (s *Store) getMemoryNodeSalienceTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (MemoryNodeSalienceRecord, bool, error) {
	var rec MemoryNodeSalienceRecord
	err := tx.QueryRowContext(
		ctx,
		`SELECT memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		   FROM memory_node_salience
		  WHERE workspace_id = ? AND memory_id = ?
		  LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(memoryID),
	).Scan(
		&rec.MemoryID,
		&rec.WorkspaceID,
		&rec.A_i,
		&rec.T_i_star,
		&rec.T_i_acc,
		&rec.N_i,
		&rec.Q_i,
		&rec.H_i,
		&rec.T_hot,
		&rec.T_warm,
		&rec.T_gc,
		&rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryNodeSalienceRecord{}, false, nil
		}
		return MemoryNodeSalienceRecord{}, false, err
	}
	return rec, true, nil
}

func memoryNodeTouchMode(trusted bool) string {
	if trusted {
		return "trusted"
	}
	return "untrusted"
}

func (s *Store) TouchMemoryNodeWithEvent(ctx context.Context, input MemoryNodeTouchInput) (MemoryNodeTouchResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return MemoryNodeTouchResult{}, errors.New("workspace_id is required")
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID == "" {
		return MemoryNodeTouchResult{}, errors.New("node_id is required")
	}
	actorType := strings.TrimSpace(input.ActorType)
	if actorType == "" {
		return MemoryNodeTouchResult{}, errors.New("actor_type is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return MemoryNodeTouchResult{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return MemoryNodeTouchResult{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, nowStr)
	if err != nil {
		return MemoryNodeTouchResult{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return MemoryNodeTouchResult{}, fmt.Errorf("begin memory node touch tx: %w", err)
	}
	defer tx.Rollback()

	var result MemoryNodeTouchResult
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		node, err := s.loadMemoryGraphNodeTx(ctx, tx, workspaceID, nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("memory graph node not found: %s/%s", workspaceID, nodeID)
		}
		if err != nil {
			return fmt.Errorf("query memory graph node: %w", err)
		}

		if input.Trusted {
			result.Salience, err = s.touchMemoryNodeTrustedTx(ctx, tx, workspaceID, nodeID, input.RiskAgent, input.SalienceConfig, now)
		} else {
			result.Salience, err = s.touchMemoryNodeUntrustedTx(ctx, tx, workspaceID, nodeID, now, true)
		}
		if err != nil {
			return err
		}

		touchMode := memoryNodeTouchMode(input.Trusted)
		fields := map[string]string{
			"workspace_id":   workspaceID,
			"memory_id":      nodeID,
			"node_id":        nodeID,
			"trusted":        fmt.Sprintf("%t", input.Trusted),
			"touch_mode":     touchMode,
			"memory_type":    strings.TrimSpace(node.MemoryType),
			"origin_kind":    strings.TrimSpace(node.OriginKind),
			"origin_id":      strings.TrimSpace(node.OriginID),
			"actor_type":     actorType,
			"actor_id":       actorID,
			"principal_type": actorType,
			"principal_id":   actorID,
		}
		payload := map[string]any{
			"workspace_id": workspaceID,
			"memory_id":    nodeID,
			"node_id":      nodeID,
			"trusted":      fmt.Sprintf("%t", input.Trusted),
			"touch_mode":   touchMode,
			"memory_type":  strings.TrimSpace(node.MemoryType),
			"origin_kind":  strings.TrimSpace(node.OriginKind),
			"origin_id":    strings.TrimSpace(node.OriginID),
			"actor_type":   actorType,
			"actor_id":     actorID,
			"summary":      "Memory node touched: " + nodeID,
		}
		payload, err = attachWorkspaceMemoryPromptContextEnvelope(payload, input.PromptContextEnvelope, "workspace.memory.node.touch", fields)
		if err != nil {
			return err
		}
		result.RuntimeEvent, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "workspace_memory.node_touched",
			EntityType:  "memory_node",
			EntityID:    nodeID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     strings.TrimSpace(node.AgentID),
			SessionID:   strings.TrimSpace(node.SessionID),
			TaskID:      strings.TrimSpace(node.TaskID),
			PayloadJSON: mustJSON(payload),
			CreatedAt:   nowStr,
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, nowStr, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after memory node touch: %w", err)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return MemoryNodeTouchResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return MemoryNodeTouchResult{}, fmt.Errorf("commit memory node touch tx: %w", err)
	}
	return result, nil
}

func (s *Store) GetMemoryNodeSalienceBatch(ctx context.Context, workspaceID string, memoryIDs []string) (map[string]MemoryNodeSalienceRecord, error) {
	if len(memoryIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(memoryIDs)+1)
	args = append(args, workspaceID)
	placeholders := make([]string, len(memoryIDs))
	for i, id := range memoryIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	query := "SELECT memory_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at FROM memory_node_salience WHERE workspace_id = ? AND memory_id IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]MemoryNodeSalienceRecord)
	for rows.Next() {
		var r MemoryNodeSalienceRecord
		r.WorkspaceID = workspaceID
		if err := rows.Scan(&r.MemoryID, &r.A_i, &r.T_i_star, &r.T_i_acc, &r.N_i, &r.Q_i, &r.H_i, &r.T_hot, &r.T_warm, &r.T_gc, &r.UpdatedAt); err == nil {
			m[r.MemoryID] = r
		}
	}
	return m, nil
}

func (s *Store) RMPRunBatchedPruning(ctx context.Context, workspaceID string, batchSize int) ([]string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	nowStr, err := s.workspaceReferenceTimestamp(ctx, workspaceID, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, nowStr)
	if err != nil {
		return nil, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var toPrune []rmpPruneCandidate
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		query := `
			SELECT s.memory_id, COALESCE(n.origin_kind, ''), COALESCE(n.origin_id, '')
			FROM memory_node_salience s
			JOIN memory_nodes n ON s.memory_id = n.memory_id AND n.workspace_id = s.workspace_id
			WHERE s.workspace_id = ?
			  AND s.t_gc <= ?
			  AND n.lifecycle_state IN ('ACTIVE', 'DORMANT', 'SUPERSEDED')
			  AND COALESCE(n.protect, 0) = 0
			  AND COALESCE(n.unresolved, 0) = 0
			  AND UPPER(COALESCE(n.memory_type, '')) NOT IN ('DISSENT', 'DISSENT_MARKER', 'DISSENT_CONTENT', 'ALTERNATIVE_BRANCH')
			LIMIT ?
		`
		rows, err := tx.QueryContext(ctx, query, workspaceID, nowStr, batchSize)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var candidate rmpPruneCandidate
			if err := rows.Scan(&candidate.MemoryID, &candidate.OriginKind, &candidate.OriginID); err != nil {
				return err
			}
			toPrune = append(toPrune, candidate)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, candidate := range toPrune {
			if strings.EqualFold(strings.TrimSpace(candidate.OriginKind), "workspace_memory") && strings.TrimSpace(candidate.OriginID) != "" {
				if _, _, _, err := s.archiveWorkspaceMemoryWithAuthorityTx(ctx, tx, WorkspaceMemoryArchiveInput{
					WorkspaceID: workspaceID,
					MemoryID:    strings.TrimSpace(candidate.OriginID),
					ArchivedBy:  rmpArchivedByPruner,
					Reason:      rmpArchivedReasonExpired,
				}, authority, nowStr); err != nil {
					return err
				}
				continue
			}
			_, err = tx.ExecContext(ctx, `
					UPDATE memory_nodes
					   SET lifecycle_state = 'ARCHIVED',
					       archived_at = ?,
					       archived_reason = CASE
					         WHEN TRIM(COALESCE(archived_reason, '')) = '' THEN ?
					         ELSE archived_reason
					       END,
					       updated_at = ?
					 WHERE workspace_id = ? AND memory_id = ?
				`, nowStr, rmpArchivedReasonExpired, nowStr, workspaceID, candidate.MemoryID)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.bestEffortReconcileMemoryProjectionWorkspace(ctx, workspaceID)
	prunedIDs := make([]string, 0, len(toPrune))
	for _, candidate := range toPrune {
		prunedIDs = append(prunedIDs, candidate.MemoryID)
	}
	return prunedIDs, nil
}
