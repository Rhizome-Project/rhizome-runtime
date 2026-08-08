package sqlite

const (
	rspCalibrationSchemaVersion = "1.0"

	rspCalibrationStatusProvisional = "PROVISIONAL"
	rspCalibrationStatusShadowOnly  = "SHADOW_ONLY"
)

type RSPCalibrationContract struct {
	SchemaVersion         string   `json:"schema_version"`
	CalibrationVersion    string   `json:"calibration_version"`
	Surface               string   `json:"surface"`
	Status                string   `json:"status"`
	Basis                 string   `json:"basis"`
	PriorSource           string   `json:"prior_source,omitempty"`
	HistoricalRowCoverage string   `json:"historical_row_coverage,omitempty"`
	Unsupported           []string `json:"unsupported,omitempty"`
	Notes                 []string `json:"notes,omitempty"`
}

type RSPTelemetryCalibrationContracts struct {
	Belief  RSPCalibrationContract `json:"belief"`
	Anomaly RSPCalibrationContract `json:"anomaly"`
	State   RSPCalibrationContract `json:"state"`
}

func rspBeliefTelemetryCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:         rspCalibrationSchemaVersion,
		CalibrationVersion:    "belief-heuristic-event-v1",
		Surface:               "rsp_belief_telemetry",
		Status:                rspCalibrationStatusProvisional,
		Basis:                 "event_type_heuristic_log_odds",
		PriorSource:           "synthetic_zero_log_odds",
		HistoricalRowCoverage: "UNVERSIONED_ROWS",
		Unsupported: []string{
			"historical_priors",
			"root_cause_independence",
			"counterfactual_updates",
		},
		Notes: []string{
			"telemetry remains shadow-only",
			"posterior updates are event-type heuristics, not calibrated belief truth",
		},
	}
}

func rspBeliefReadModelCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:      rspCalibrationSchemaVersion,
		CalibrationVersion: "belief-read-model-v2",
		Surface:            "rsp_belief_report",
		Status:             rspCalibrationStatusProvisional,
		Basis:              "claim_read_model_plus_root_cause_aware_belief_scoring",
		PriorSource:        "claim_confidence_status_evidence_shape_and_runtime_lineage",
		Unsupported: []string{
			"historical_priors",
			"counterfactual_updates",
			"global_root_cause_coverage",
		},
		Notes: []string{
			"belief outputs are advisory shadow signals",
			"root-cause collapse applies only when runtime lineage is discoverable",
		},
	}
}

func rspAnomalyTelemetryCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:         rspCalibrationSchemaVersion,
		CalibrationVersion:    "anomaly-ewma-shrinkage-v1",
		Surface:               "rsp_anomaly_telemetry",
		Status:                rspCalibrationStatusShadowOnly,
		Basis:                 "ewma_baselines_with_scope_shrinkage",
		HistoricalRowCoverage: "MIXED_DURING_MIGRATION",
		Unsupported: []string{
			"root_cause_independence",
			"explicit_intervention_modeling",
		},
		Notes: []string{
			"anomaly telemetry is tuned for observability, not sovereign actuation",
		},
	}
}

func rspStateTelemetryCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:         rspCalibrationSchemaVersion,
		CalibrationVersion:    "state-shadow-s1-v1",
		Surface:               "rsp_state_telemetry",
		Status:                rspCalibrationStatusShadowOnly,
		Basis:                 "shadow_state_read_model_rollup",
		HistoricalRowCoverage: "UNVERSIONED_ROWS",
		Unsupported: []string{
			"root_cause_independence",
			"intervention_counterfactuals",
		},
		Notes: []string{
			"state telemetry remains shadow-only and non-sovereign",
		},
	}
}

func rspStateReadModelCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:      rspCalibrationSchemaVersion,
		CalibrationVersion: "state-read-model-v2",
		Surface:            "rsp_state_report",
		Status:             rspCalibrationStatusShadowOnly,
		Basis:              "state_shadow_read_model_rollup_with_root_cause_collapse",
		Unsupported: []string{
			"intervention_counterfactuals",
			"global_root_cause_coverage",
		},
		Notes: []string{
			"state outputs remain shadow-only and non-sovereign",
			"root-cause collapse applies only when runtime lineage is discoverable",
		},
	}
}

func rspForecastCalibrationContract() RSPCalibrationContract {
	return RSPCalibrationContract{
		SchemaVersion:      rspCalibrationSchemaVersion,
		CalibrationVersion: "forecast-shadow-s2-v2",
		Surface:            "rsp_forecast_report",
		Status:             rspCalibrationStatusProvisional,
		Basis:              "shadow_state_plus_metrics_and_shared_latency_projection_with_bounded_control_plan_inputs",
		Unsupported: []string{
			"root_cause_independence",
			"historical_intervention_debiasing",
			"broad_external_non_control_inputs",
			"causal_intervention_counterfactuals",
			"intervention_lift_evaluation",
		},
		Notes: []string{
			"forecast outputs remain bounded shadow forecasts",
			"non-control exogenous coverage is currently limited to persisted shared-latency history",
			"scenario conditioning applies only to explicit persisted pending effective-controls inputs",
			"active control mode is surfaced as current-state context, not as a planned intervention delta",
			"baseline forecast readiness remains separate from scenario readiness",
		},
	}
}

func rspTelemetryCalibrationContracts() RSPTelemetryCalibrationContracts {
	return RSPTelemetryCalibrationContracts{
		Belief:  rspBeliefTelemetryCalibrationContract(),
		Anomaly: rspAnomalyTelemetryCalibrationContract(),
		State:   rspStateTelemetryCalibrationContract(),
	}
}
