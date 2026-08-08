package sqlite

import (
	"context"
	"database/sql"
	"math"
	"time"
)

type RSPForecastStateRecord struct {
	WorkspaceID    string  `json:"workspace_id"`
	ProtoClusterID string  `json:"proto_cluster_id"`
	AgentID        string  `json:"agent_id"`
	MetricName     string  `json:"metric_name"`
	Level          float64 `json:"l_k"`
	Trend          float64 `json:"b_k"`
	Variance       float64 `json:"v_k"`
	Sigma          float64 `json:"sigma_k"`
	Persistence    float64 `json:"p_k"`
	Alpha          float64 `json:"alpha_k"`
	Beta           float64 `json:"beta_k"`
	LastY          float64 `json:"last_y"`
	LastYTilde     float64 `json:"last_y_tilde"`
	UpdatedAt      string  `json:"updated_at"`
}

type RSPDampedHoltConfig struct {
	Phi      float64
	AlphaMin float64
	AlphaMax float64
	BetaMin  float64
	BetaMax  float64
	RhoV     float64
	RhoSigma float64
	RhoP     float64
	ThetaD   float64
}

func DefaultRSPDampedHoltConfig() RSPDampedHoltConfig {
	return RSPDampedHoltConfig{
		Phi:      0.85,
		AlphaMin: 0.05,
		AlphaMax: 0.3,
		BetaMin:  0.01,
		BetaMax:  0.1,
		RhoV:     0.9,
		RhoSigma: 0.9,
		RhoP:     0.8,
		ThetaD:   2.0,
	}
}

func (s *Store) RSPUpdateForecastState(ctx context.Context, rec RSPForecastStateRecord, yNew float64, gammaU float64, gateGk float64, cfg RSPDampedHoltConfig) error {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existing RSPForecastStateRecord
	err = tx.QueryRowContext(ctx, `SELECT l_k, b_k, v_k, sigma_k, p_k, alpha_k, beta_k, last_y_tilde FROM rsp_forecast_state WHERE workspace_id = ? AND proto_cluster_id = ? AND agent_id = ? AND metric_name = ?`,
		rec.WorkspaceID, rec.ProtoClusterID, rec.AgentID, rec.MetricName).Scan(
		&existing.Level, &existing.Trend, &existing.Variance, &existing.Sigma, &existing.Persistence, &existing.Alpha, &existing.Beta, &existing.LastYTilde)

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if err == sql.ErrNoRows {
		existing = RSPForecastStateRecord{
			Level: yNew, Trend: 0, Variance: 1.0, Sigma: 1.0, Persistence: 0,
			Alpha: cfg.AlphaMin, Beta: cfg.BetaMin, LastYTilde: yNew,
		}
	}

	yTilde := yNew - gammaU

	// Innovations
	e_k := yTilde - (existing.Level + cfg.Phi*existing.Trend)

	// Robust scale
	newSigma := cfg.RhoSigma*existing.Sigma + (1.0-cfg.RhoSigma)*math.Abs(e_k)
	newVar := cfg.RhoV*existing.Variance + (1.0-cfg.RhoV)*(e_k*e_k)

	// Normalized deriv
	d_k := math.Abs(yTilde-existing.LastYTilde) / (newSigma + 1e-6) // avoid div by 0

	// Persistence
	indic := 0.0
	if d_k > cfg.ThetaD {
		indic = 1.0
	}
	newP := cfg.RhoP*existing.Persistence + (1.0-cfg.RhoP)*indic

	// Adaptive gate (g_k) updates Alpha and Beta
	newAlpha := cfg.AlphaMin + (cfg.AlphaMax-cfg.AlphaMin)*gateGk
	newBeta := cfg.BetaMin + (cfg.BetaMax-cfg.BetaMin)*gateGk*newP

	// Stability bounds check
	if newBeta > newAlpha/2.0 {
		newBeta = newAlpha / 2.0
	}

	// Damped Holt Step Update
	newL := newAlpha*yTilde + (1.0-newAlpha)*(existing.Level+cfg.Phi*existing.Trend)
	newB := newBeta*(newL-existing.Level) + (1.0-newBeta)*cfg.Phi*existing.Trend

	_, err = tx.ExecContext(ctx, `
		INSERT INTO rsp_forecast_state (workspace_id, proto_cluster_id, agent_id, metric_name, l_k, b_k, v_k, sigma_k, p_k, alpha_k, beta_k, last_y, last_y_tilde, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, proto_cluster_id, agent_id, metric_name) DO UPDATE SET
			l_k=excluded.l_k,
			b_k=excluded.b_k,
			v_k=excluded.v_k,
			sigma_k=excluded.sigma_k,
			p_k=excluded.p_k,
			alpha_k=excluded.alpha_k,
			beta_k=excluded.beta_k,
			last_y=excluded.last_y,
			last_y_tilde=excluded.last_y_tilde,
			updated_at=excluded.updated_at
	`, rec.WorkspaceID, rec.ProtoClusterID, rec.AgentID, rec.MetricName, newL, newB, newVar, newSigma, newP, newAlpha, newBeta, yNew, yTilde, nowStr)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) ListRSPForecastsByCluster(ctx context.Context, workspaceID, protoClusterID string) ([]RSPForecastStateRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT agent_id, metric_name, l_k, b_k, v_k, sigma_k, p_k, alpha_k, beta_k, last_y, last_y_tilde, updated_at
		FROM rsp_forecast_state
		WHERE workspace_id = ? AND proto_cluster_id = ?
	`, workspaceID, protoClusterID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var forecasts []RSPForecastStateRecord
	for rows.Next() {
		var f RSPForecastStateRecord
		f.WorkspaceID = workspaceID
		f.ProtoClusterID = protoClusterID
		if err := rows.Scan(
			&f.AgentID, &f.MetricName,
			&f.Level, &f.Trend, &f.Variance, &f.Sigma, &f.Persistence,
			&f.Alpha, &f.Beta, &f.LastY, &f.LastYTilde, &f.UpdatedAt,
		); err == nil {
			forecasts = append(forecasts, f)
		}
	}
	return forecasts, nil
}
