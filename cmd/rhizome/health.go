package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type configSnapshot struct {
	DBPath               string `json:"db_path,omitempty"`
	WorkspaceRoot        string `json:"workspace_root,omitempty"`
	MetricsPath          string `json:"metrics_path,omitempty"`
	ExecutorPython       string `json:"executor_python,omitempty"`
	ExecutorBridgeScript string `json:"executor_bridge_script,omitempty"`
}

type serviceMetricsSummary struct {
	Status              string               `json:"status"`
	SourcePath          string               `json:"source_path,omitempty"`
	SnapshotsLoaded     int                  `json:"snapshots_loaded"`
	SnapshotsTotalValid int                  `json:"snapshots_total_valid"`
	ParseErrors         int                  `json:"parse_errors"`
	LatestTimestamp     string               `json:"latest_timestamp,omitempty"`
	Health              runtimeMetricsHealth `json:"health"`
	Error               string               `json:"error,omitempty"`
}

type serviceLivenessPayload struct {
	Status string `json:"status"`
	TS     string `json:"ts"`
}

type serviceHealthPayload struct {
	Status               string                                          `json:"status"`
	TS                   string                                          `json:"ts"`
	Semantics            TopLevelSemantics                               `json:"semantics"`
	RuntimeWorkGate      *RuntimeWorkGateDiagnostics                     `json:"runtime_work_gate,omitempty"`
	PromptAuthority      PromptAuthorityScopeDiagnostics                 `json:"prompt_authority_scope"`
	Config               configSnapshot                                  `json:"config"`
	Runtime              app.RuntimeBuildInfo                            `json:"runtime"`
	Checkout             app.GitCheckoutInfo                             `json:"checkout"`
	Metrics              serviceMetricsSummary                           `json:"metrics"`
	DurableRuntime       *durableRuntimeSnapshot                         `json:"durable_runtime,omitempty"`
	AuthorityNode        sqlite.AuthorityNodeDiagnostics                 `json:"authority_node,omitempty"`
	AuthorityLease       sqlite.AuthorityLeaseDiagnostics                `json:"authority_lease,omitempty"`
	LoopReadiness        []LoopReadiness                                 `json:"loop_readiness,omitempty"`
	StuckAgentsHealth    *sqlite.StuckAgentSnapshot                      `json:"stuck_agents_health,omitempty"`
	NoProgressHealth     *sqlite.ExecutionNoProgressSnapshot             `json:"no_progress_health,omitempty"`
	BudgetLedger         *sqlite.BudgetLedgerHealthSnapshot              `json:"budget_ledger,omitempty"`
	PatchQueue           *sqlite.ProjectPatchQueueDurabilityProof        `json:"project_patch_queue_durability,omitempty"`
	RepoMutationActuator *sqlite.ProjectPatchQueueActuatorHealthSnapshot `json:"repo_mutation_actuator,omitempty"`
	RepoMutation         repoauthority.MutationActivationGateResult      `json:"repo_mutation_activation"`
	RepoMutationDryRun   repoauthority.MutationActuatorDryRunResult      `json:"repo_mutation_actuator_dry_run"`
	Extended             ExtendedReadiness                               `json:"extended_readiness"`
}

type DiagnosticSignal struct {
	State   string `json:"state"`
	Message string `json:"message,omitempty"`
}

type RuntimeBootstrapWorkFallbackDiagnostics struct {
	Posture        string `json:"posture,omitempty"`
	GateState      string `json:"gate_state,omitempty"`
	Mode           string `json:"mode,omitempty"`
	Selector       string `json:"selector,omitempty"`
	Scope          string `json:"scope,omitempty"`
	CanConsumeWork bool   `json:"can_consume_work"`
	Summary        string `json:"summary,omitempty"`
}

type RuntimeWorkGateDiagnostics struct {
	WorkType              string                                  `json:"work_type,omitempty"`
	WorkCoordinationState string                                  `json:"work_coordination_state,omitempty"`
	WorkGateState         string                                  `json:"work_gate_state,omitempty"`
	WorkGateType          string                                  `json:"work_gate_type,omitempty"`
	WorkGateReason        string                                  `json:"work_gate_reason,omitempty"`
	WorkGateNeededFrom    string                                  `json:"work_gate_needed_from,omitempty"`
	WorkGateSummary       string                                  `json:"work_gate_summary,omitempty"`
	ProfileGateState      string                                  `json:"profile_gate_state,omitempty"`
	ProfileGateSummary    string                                  `json:"profile_gate_summary,omitempty"`
	BootstrapWorkFallback RuntimeBootstrapWorkFallbackDiagnostics `json:"bootstrap_work_fallback"`
}

type PromptAuthorityScopeDiagnostics struct {
	State    string                           `json:"state"`
	Contract string                           `json:"contract"`
	Message  string                           `json:"message"`
	Surfaces []PromptAuthoritySurfaceBoundary `json:"surfaces"`
}

type PromptAuthoritySurfaceBoundary struct {
	Surface                               string   `json:"surface"`
	Decision                              string   `json:"decision"`
	AuthorityBoundary                     string   `json:"authority_boundary"`
	PromptCompilerStatus                  string   `json:"prompt_compiler_status"`
	C21Convergence                        string   `json:"c2_1_convergence"`
	DeploymentEvidence                    string   `json:"deployment_evidence"`
	FirstDeploymentPreflight              string   `json:"first_deployment_preflight"`
	AcceptedAsDaemonConvergence           bool     `json:"accepted_as_daemon_convergence"`
	RequiresPromptEnvelopeBeforePromotion bool     `json:"requires_prompt_envelope_before_promotion"`
	Evidence                              []string `json:"evidence,omitempty"`
}

type TopLevelSemantics struct {
	Liveness            DiagnosticSignal `json:"liveness"`
	Readiness           DiagnosticSignal `json:"readiness"`
	DeploymentReadiness DiagnosticSignal `json:"deployment_readiness"`
	Degraded            DiagnosticSignal `json:"degraded"`
}

type ExtendedReadiness struct {
	MotifLifecycle         DiagnosticSignal                      `json:"motif_lifecycle"`
	InvalidationLag        DiagnosticSignal                      `json:"invalidation_lag"`
	OperatorQueueLag       DiagnosticSignal                      `json:"operator_queue_lag"`
	ReviewerScarcity       DiagnosticSignal                      `json:"reviewer_scarcity"`
	ReviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot `json:"reviewer_scarcity_health,omitempty"`
	StuckAgents            DiagnosticSignal                      `json:"stuck_agents"`
	NoProgress             DiagnosticSignal                      `json:"no_progress"`
	ProjectionLag          sqlite.MemoryProjectionLagSnapshot    `json:"projection_lag"`
	ReplayHealth           DiagnosticSignal                      `json:"replay_health"`
}

type durableRuntimeSnapshot struct {
	State                     string                         `json:"state,omitempty"`
	Message                   string                         `json:"message,omitempty"`
	ReferenceAt               string                         `json:"reference_at,omitempty"`
	WorkspaceID               string                         `json:"workspace_id,omitempty"`
	RunID                     string                         `json:"run_id,omitempty"`
	SessionID                 string                         `json:"session_id,omitempty"`
	TaskID                    string                         `json:"task_id,omitempty"`
	AgentID                   string                         `json:"agent_id,omitempty"`
	RunStatus                 string                         `json:"run_status,omitempty"`
	RunOutcome                string                         `json:"run_outcome,omitempty"`
	RunTitle                  string                         `json:"run_title,omitempty"`
	RunSummary                string                         `json:"run_summary,omitempty"`
	StepID                    string                         `json:"step_id,omitempty"`
	StepPhase                 string                         `json:"step_phase,omitempty"`
	StepTitle                 string                         `json:"step_title,omitempty"`
	StepStatus                string                         `json:"step_status,omitempty"`
	StepSummary               string                         `json:"step_summary,omitempty"`
	Progress                  string                         `json:"progress,omitempty"`
	OperationID               string                         `json:"operation_id,omitempty"`
	OperationName             string                         `json:"operation_name,omitempty"`
	OperationKind             string                         `json:"operation_kind,omitempty"`
	OperationStatus           string                         `json:"operation_status,omitempty"`
	OperationUpdatedAt        string                         `json:"operation_updated_at,omitempty"`
	OperationBindingRunID     string                         `json:"operation_binding_run_id,omitempty"`
	OperationBindingSessionID string                         `json:"operation_binding_session_id,omitempty"`
	OperationBindingTaskID    string                         `json:"operation_binding_task_id,omitempty"`
	OperationBindingAgentID   string                         `json:"operation_binding_agent_id,omitempty"`
	SessionStatus             string                         `json:"session_status,omitempty"`
	SessionSummary            string                         `json:"session_summary,omitempty"`
	SessionUpdatedAt          string                         `json:"session_updated_at,omitempty"`
	Evidence                  []string                       `json:"evidence,omitempty"`
	PromptCompiler            *durablePromptCompilerSnapshot `json:"prompt_compiler,omitempty"`
	Issues                    []string                       `json:"issues,omitempty"`
}

type durablePromptCompilerSnapshot struct {
	State                  string `json:"state,omitempty"`
	Message                string `json:"message,omitempty"`
	StepID                 string `json:"step_id,omitempty"`
	StepPhase              string `json:"step_phase,omitempty"`
	Contract               string `json:"contract,omitempty"`
	CapabilitySnapshotID   string `json:"capability_snapshot_id,omitempty"`
	CapabilitySnapshotRef  string `json:"capability_snapshot_ref,omitempty"`
	CapabilitySnapshotPath string `json:"capability_snapshot_path,omitempty"`
	ProjectionSource       string `json:"projection_source,omitempty"`
	ProjectionContract     string `json:"projection_contract,omitempty"`
	ProjectionDigest       string `json:"projection_digest,omitempty"`
	SnapshotReadbackState  string `json:"snapshot_readback_state,omitempty"`
	SnapshotReadbackDigest string `json:"snapshot_readback_digest,omitempty"`
}

const (
	durablePromptCapabilityEvidenceContract   = "daemon_prompt_capability_evidence.v1"
	durablePromptCompilerStatusConverged      = "daemon_converged"
	durablePromptConvergenceAccepted          = "daemon_prompt_compiler_converged"
	durablePromptDeploymentEvidenceAccepted   = "accepted_for_daemon_prompt_compiler_convergence"
	durablePromptProjectionSource             = "agent.runtime_capability_snapshot"
	durablePromptProjectionContract           = "active_capability_snapshot_projection.v1"
	durablePromptSnapshotSchema               = "daemon_capability_snapshot.v1"
	durablePromptSnapshotKind                 = "run"
	durablePromptSnapshotStatus               = "enabled"
	durablePromptContractID                   = "prompt_capabilities.v1"
	promptAuthorityScopeContract              = "first_stable_prompt_authority_scope.v1"
	promptAuthorityStatusExcluded             = "excluded_until_migrated"
	promptAuthorityDeploymentEvidenceRejected = "not_accepted_for_daemon_prompt_compiler_convergence"
)

func collectPublicLivenessPayload() serviceLivenessPayload {
	return serviceLivenessPayload{
		Status: "ok",
		TS:     time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func snapshotConfig(cfg app.Config) configSnapshot {
	return configSnapshot{
		DBPath:               strings.TrimSpace(cfg.DBPath),
		WorkspaceRoot:        strings.TrimSpace(cfg.WorkspaceRoot),
		MetricsPath:          strings.TrimSpace(cfg.MetricsPath),
		ExecutorPython:       strings.TrimSpace(cfg.ExecutorPython),
		ExecutorBridgeScript: strings.TrimSpace(cfg.ExecutorBridgeScript),
	}
}

func collectServiceHealthPayload(cfg app.Config, registry *ReadinessRegistry, projectionLag sqlite.MemoryProjectionLagSnapshot) serviceHealthPayload {
	return collectServiceHealthPayloadWithAuthority(
		cfg,
		registry,
		projectionLag,
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"},
		sqlite.AuthorityNodeDiagnostics{},
		sqlite.AuthorityLeaseDiagnostics{},
	)
}

func collectServiceHealthPayloadWithAuthority(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	stuckAgents DiagnosticSignal,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
) serviceHealthPayload {
	return collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		sqlite.ReviewerScarcityHealthSnapshot{},
		stuckAgents,
		authority,
		authorityLease,
	)
}

func collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealth(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
) serviceHealthPayload {
	return collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcityHealth,
		stuckAgents,
		nil,
		authority,
		authorityLease,
	)
}

func collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealth(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	stuckAgentsHealth *sqlite.StuckAgentSnapshot,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
) serviceHealthPayload {
	return collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealthAndNoProgressHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcityHealth,
		stuckAgents,
		stuckAgentsHealth,
		DiagnosticSignal{},
		nil,
		authority,
		authorityLease,
	)
}

func collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealthAndNoProgressHealth(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	stuckAgentsHealth *sqlite.StuckAgentSnapshot,
	noProgress DiagnosticSignal,
	noProgressHealth *sqlite.ExecutionNoProgressSnapshot,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
) serviceHealthPayload {
	return collectServiceHealthPayloadFromStateWithReviewerScarcityHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcityHealth,
		stuckAgents,
		stuckAgentsHealth,
		noProgress,
		noProgressHealth,
		authority,
		authorityLease,
		app.CurrentRuntimeBuildInfo(),
		app.CurrentGitCheckoutInfo(),
	)
}

func collectServiceHealthPayloadFromState(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	stuckAgents DiagnosticSignal,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
	runtimeInfo app.RuntimeBuildInfo,
	checkout app.GitCheckoutInfo,
) serviceHealthPayload {
	return collectServiceHealthPayloadFromStateWithReviewerScarcityHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		sqlite.ReviewerScarcityHealthSnapshot{},
		stuckAgents,
		nil,
		DiagnosticSignal{},
		nil,
		authority,
		authorityLease,
		runtimeInfo,
		checkout,
	)
}

func collectServiceHealthPayloadFromStateWithReviewerScarcityHealth(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	stuckAgentsHealth *sqlite.StuckAgentSnapshot,
	noProgress DiagnosticSignal,
	noProgressHealth *sqlite.ExecutionNoProgressSnapshot,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
	runtimeInfo app.RuntimeBuildInfo,
	checkout app.GitCheckoutInfo,
) serviceHealthPayload {
	return collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealth(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcityHealth,
		stuckAgents,
		stuckAgentsHealth,
		noProgress,
		noProgressHealth,
		authority,
		authorityLease,
		runtimeInfo,
		checkout,
	)
}

func collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealth(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	stuckAgentsHealth *sqlite.StuckAgentSnapshot,
	noProgress DiagnosticSignal,
	noProgressHealth *sqlite.ExecutionNoProgressSnapshot,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
	runtimeInfo app.RuntimeBuildInfo,
	checkout app.GitCheckoutInfo,
) serviceHealthPayload {
	return collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealthAndBudgetLedger(
		cfg,
		registry,
		projectionLag,
		operatorQueueLag,
		reviewerScarcity,
		reviewerScarcityHealth,
		stuckAgents,
		stuckAgentsHealth,
		noProgress,
		noProgressHealth,
		authority,
		authorityLease,
		nil,
		runtimeInfo,
		checkout,
	)
}

func collectServiceHealthPayloadFromStateWithReviewerScarcityHealthAndNoProgressHealthAndBudgetLedger(
	cfg app.Config,
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	stuckAgentsHealth *sqlite.StuckAgentSnapshot,
	noProgress DiagnosticSignal,
	noProgressHealth *sqlite.ExecutionNoProgressSnapshot,
	authority sqlite.AuthorityNodeDiagnostics,
	authorityLease sqlite.AuthorityLeaseDiagnostics,
	budgetLedger *sqlite.BudgetLedgerHealthSnapshot,
	runtimeInfo app.RuntimeBuildInfo,
	checkout app.GitCheckoutInfo,
) serviceHealthPayload {
	durableRuntime := collectDurableRuntimeSnapshot(cfg.DBPath)
	payload := serviceHealthPayload{
		Status:            "ok",
		TS:                time.Now().UTC().Format(time.RFC3339Nano),
		PromptAuthority:   collectPromptAuthorityScopeDiagnostics(),
		Config:            snapshotConfig(cfg),
		Runtime:           runtimeInfo,
		Checkout:          checkout,
		Metrics:           collectServiceMetricsSummary(cfg.MetricsPath),
		DurableRuntime:    durableRuntime,
		AuthorityNode:     authority,
		AuthorityLease:    authorityLease,
		StuckAgentsHealth: stuckAgentsHealth,
		NoProgressHealth:  noProgressHealth,
		BudgetLedger:      budgetLedger,
		RepoMutation:      collectRepoMutationActivationDiagnostics(),
		Extended:          evaluateExtendedReadiness(registry, projectionLag, operatorQueueLag, reviewerScarcity, reviewerScarcityHealth, stuckAgents, noProgress),
	}
	payload.RepoMutationDryRun = collectRepoMutationActuatorDryRunDiagnostics(payload.RepoMutation)

	degradedReasons := make([]string, 0, 6)
	overallLoopState := LoopRunning
	if registry != nil {
		payload.LoopReadiness = registry.Snapshot()
		overallLoopState = registry.OverallState()
		switch overallLoopState {
		case LoopRecovering, LoopDegraded:
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, "one or more loops are degraded")
		case LoopNotStarted:
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, "one or more required loops have not started")
		case LoopStopped:
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, "one or more required loops stopped")
		}
	}

	if payload.Checkout.Error != "" || payload.Runtime.VCSModified {
		payload.Status = "degraded"
		if payload.Checkout.Error != "" {
			degradedReasons = append(degradedReasons, "current git checkout is unavailable")
		}
		if payload.Runtime.VCSModified {
			degradedReasons = append(degradedReasons, "runtime binary was built from a modified checkout")
		}
	}
	if runtimeRevisionDrifts(runtimeInfo, checkout) {
		payload.Status = "degraded"
		degradedReasons = append(
			degradedReasons,
			fmt.Sprintf(
				"runtime vcs_revision %s differs from checkout head %s",
				shortRevision(runtimeInfo.VCSRevision),
				shortRevision(checkout.Head),
			),
		)
	}
	switch payload.Metrics.Status {
	case "degraded", "missing", "error":
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("runtime metrics status is %s", payload.Metrics.Status))
	case "unknown":
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, "runtime metrics health is unknown")
	}
	if payload.Extended.ProjectionLag.State == "degraded" || strings.TrimSpace(payload.Extended.ProjectionLag.Error) != "" {
		payload.Status = "degraded"
		if strings.TrimSpace(payload.Extended.ProjectionLag.Error) != "" {
			degradedReasons = append(degradedReasons, "projection lag collection failed")
		} else {
			degradedReasons = append(degradedReasons, "projection lag is degraded")
		}
	}
	if payload.Extended.OperatorQueueLag.State == "degraded" || payload.Extended.OperatorQueueLag.State == "error" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("operator queue lag state is %s", payload.Extended.OperatorQueueLag.State))
	}
	if payload.Extended.ReviewerScarcity.State == "degraded" || payload.Extended.ReviewerScarcity.State == "error" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("reviewer scarcity state is %s", payload.Extended.ReviewerScarcity.State))
	}
	if payload.Extended.StuckAgents.State == "degraded" || payload.Extended.StuckAgents.State == "error" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("stuck agent state is %s", payload.Extended.StuckAgents.State))
	}
	if payload.Extended.NoProgress.State == "blocked" || payload.Extended.NoProgress.State == "needs_operator" || payload.Extended.NoProgress.State == "degraded" || payload.Extended.NoProgress.State == "error" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("no-progress state is %s", payload.Extended.NoProgress.State))
	}
	if durableRuntime != nil {
		switch strings.ToLower(strings.TrimSpace(durableRuntime.State)) {
		case "missing", "mismatch", "error":
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, "durable runtime readback is "+strings.ToLower(strings.TrimSpace(durableRuntime.State)))
		}
		if durablePromptCompilerConvergenceBlocksServeReadiness(payload.LoopReadiness, durableRuntime.PromptCompiler) {
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, "daemon prompt compiler convergence is "+strings.ToLower(strings.TrimSpace(durableRuntime.PromptCompiler.State)))
		}
	}
	if payload.AuthorityNode.State == "missing" || payload.AuthorityNode.State == "error" || payload.AuthorityNode.State == "degraded" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("authority node state is %s", payload.AuthorityNode.State))
	}
	if payload.AuthorityLease.State == "degraded" || payload.AuthorityLease.State == "error" || payload.AuthorityLease.State == "missing" {
		payload.Status = "degraded"
		degradedReasons = append(degradedReasons, fmt.Sprintf("authority lease state is %s", payload.AuthorityLease.State))
	}
	if payload.BudgetLedger != nil {
		switch strings.ToLower(strings.TrimSpace(payload.BudgetLedger.Status)) {
		case "degraded", "exhausted", "error":
			payload.Status = "degraded"
			degradedReasons = append(degradedReasons, budgetLedgerDegradedReason(*payload.BudgetLedger))
		}
	}
	readiness := DiagnosticSignal{State: "ok", Message: "core dependencies initialized"}
	if registry != nil {
		switch overallLoopState {
		case LoopRecovering:
			readiness = DiagnosticSignal{State: "not_ready", Message: "one or more loops are recovering"}
		case LoopNotStarted:
			readiness = DiagnosticSignal{State: "not_ready", Message: "one or more required loops have not started"}
		case LoopStopped:
			readiness = DiagnosticSignal{State: "not_ready", Message: "one or more required loops stopped"}
		}
	}

	deploymentReadiness := DiagnosticSignal{State: "ok", Message: "deployment diagnostics are ready"}
	switch {
	case readiness.State != "ok":
		deploymentReadiness = DiagnosticSignal{
			State:   readiness.State,
			Message: readiness.Message,
		}
	case payload.Status == "degraded":
		deploymentReadiness = DiagnosticSignal{
			State:   "degraded",
			Message: degradedMessage(degradedReasons),
		}
	}

	// Calculate Top-Level Structured Semantics
	payload.Semantics = TopLevelSemantics{
		Liveness:            DiagnosticSignal{State: "ok", Message: "endpoint is reachable"},
		Readiness:           readiness,
		DeploymentReadiness: deploymentReadiness,
		Degraded:            DiagnosticSignal{State: "ok", Message: "no known degradation"},
	}

	if payload.Status == "degraded" {
		payload.Semantics.Degraded = DiagnosticSignal{
			State:   "degraded",
			Message: degradedMessage(degradedReasons),
		}
	}

	return payload
}

func durablePromptCompilerConvergenceBlocksServeReadiness(loops []LoopReadiness, snapshot *durablePromptCompilerSnapshot) bool {
	if snapshot == nil || daemonLoopDisabled(loops) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(snapshot.State)) {
	case "missing", "mismatch", "error":
		return true
	default:
		return false
	}
}

func daemonLoopDisabled(loops []LoopReadiness) bool {
	for _, loop := range loops {
		if strings.TrimSpace(loop.Name) == loopNameDaemon {
			return strings.EqualFold(strings.TrimSpace(string(loop.State)), string(LoopDisabled))
		}
	}
	return false
}

func collectRepoMutationActivationDiagnostics(proofs ...*sqlite.ProjectPatchQueueDurabilityProof) repoauthority.MutationActivationGateResult {
	return repoauthority.EvaluateMutationActivationGates(repoauthority.MutationActivationGateInput{
		AuthorityMode:       repoauthority.ModePatchOnlyTempRepo,
		DirectMergeDisabled: true,
		QueueDurable:        projectPatchQueueProofIsDurable(firstProjectPatchQueueProof(proofs...)),
		ReviewerMeshMode:    repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		Source:              repoauthority.MutationActivationSourceSyntheticPatchOnly,
	})
}

func collectRepoMutationActuatorDryRunDiagnostics(activation repoauthority.MutationActivationGateResult) repoauthority.MutationActuatorDryRunResult {
	return repoauthority.EvaluateMutationActuatorDryRun(repoauthority.MutationActuatorDryRunInput{
		Activation: activation,
	})
}

func collectRepoMutationActivationDiagnosticsFromStore(ctx context.Context, store *sqlite.Store, proof *sqlite.ProjectPatchQueueDurabilityProof) repoauthority.MutationActivationGateResult {
	queueDurable := projectPatchQueueProofIsDurable(proof)
	if !queueDurable || store == nil {
		return collectRepoMutationActivationDiagnostics(proof)
	}
	candidate, ok, err := store.FirstProjectRepoMutationActivationCandidate(ctx)
	if err != nil {
		return repoauthority.EvaluateMutationActivationGates(repoauthority.MutationActivationGateInput{
			AuthorityMode:       repoauthority.ModePatchOnlyTempRepo,
			DirectMergeDisabled: true,
			QueueDurable:        queueDurable,
			ReviewerMeshMode:    repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
			Source:              repoauthority.MutationActivationSourceDurableControlledQueueCandidateError,
			SourceError:         err.Error(),
		})
	}
	if !ok {
		return repoauthority.EvaluateMutationActivationGates(repoauthority.MutationActivationGateInput{
			AuthorityMode:       repoauthority.ModePatchOnlyTempRepo,
			DirectMergeDisabled: true,
			QueueDurable:        queueDurable,
			ReviewerMeshMode:    repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
			Source:              repoauthority.MutationActivationSourceDurableQueueNoControlledCandidate,
		})
	}
	return collectRepoMutationActivationDiagnosticsFromCandidateWithContext(ctx, proof, candidate, true)
}

func collectRepoMutationActivationDiagnosticsFromCandidate(proof *sqlite.ProjectPatchQueueDurabilityProof, candidate sqlite.ProjectRepoMutationActivationCandidate, ok bool) repoauthority.MutationActivationGateResult {
	return collectRepoMutationActivationDiagnosticsFromCandidateWithContext(context.Background(), proof, candidate, ok)
}

func collectRepoMutationActivationDiagnosticsFromCandidateWithContext(ctx context.Context, proof *sqlite.ProjectPatchQueueDurabilityProof, candidate sqlite.ProjectRepoMutationActivationCandidate, ok bool) repoauthority.MutationActivationGateResult {
	if !ok || !projectPatchQueueProofIsDurable(proof) {
		return collectRepoMutationActivationDiagnostics(proof)
	}
	liveVerifierEnabled, liveVerifierSource := repoMutationLiveVerifierState()
	return repoauthority.EvaluateMutationActivationGates(repoauthority.MutationActivationGateInput{
		AuthorityMode:               repoauthority.ModeControlledQueue,
		DirectMergeDisabled:         true,
		QueueDurable:                projectPatchQueueProofIsDurable(proof),
		ReviewerMeshMode:            repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		LiveMutationVerifierEnabled: liveVerifierEnabled,
		LiveMutationVerifierSource:  liveVerifierSource,
		Source:                      repoauthority.MutationActivationSourceDurableControlledQueueCandidate,
		Candidate:                   repoMutationCandidateSummary(candidate),
		WorktreeIdentity:            repoMutationWorktreeIdentityFromCandidate(ctx, candidate),
		TargetWorktreeIdentity:      repoMutationTargetWorktreeIdentityFromCandidate(ctx, candidate),
		Context:                     repoMutationContextFromCandidate(candidate),
		PatchQueueItem:              repoMutationPatchQueueItemFromCandidate(candidate),
		RollbackEvidence:            repoMutationRollbackEvidenceFromCandidate(candidate),
		ReviewerAdvisory:            repoMutationReviewerAdvisoryFromCandidate(candidate),
		OperatorEnablement:          repoMutationOperatorEnablementFromCandidate(candidate),
		PatchMaterialization:        repoMutationPatchMaterializationFromCandidate(candidate),
		PatchMaterializationProof:   repoMutationPatchMaterializationAuthorityProofFromCandidate(candidate),
	})
}

func repoMutationLiveVerifierState() (bool, string) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_REPO_MUTATION_LIVE_VERIFIER"))) {
	case "1", "true", "yes", "on", "enabled":
		return true, repoauthority.MutationActivationLiveVerifierSourceEnv
	default:
		return false, ""
	}
}

func repoMutationCandidateSummary(candidate sqlite.ProjectRepoMutationActivationCandidate) *repoauthority.MutationActivationCandidateSummary {
	item := candidate.QueueItem
	branch := candidate.Branch
	checkout := candidate.Checkout
	target := candidate.TargetCheckout
	return &repoauthority.MutationActivationCandidateSummary{
		WorkspaceID:      strings.TrimSpace(item.WorkspaceID),
		ProjectID:        strings.TrimSpace(item.ProjectID),
		RepoID:           strings.TrimSpace(item.RepoID),
		QueueID:          strings.TrimSpace(item.QueueID),
		ItemID:           strings.TrimSpace(item.ItemID),
		BranchID:         strings.TrimSpace(item.BranchID),
		BranchName:       firstNonEmpty(branch.BranchName, checkout.BranchName),
		CheckoutID:       strings.TrimSpace(checkout.CheckoutID),
		TargetCheckoutID: strings.TrimSpace(target.CheckoutID),
		TargetBranchName: strings.TrimSpace(target.BranchName),
		State:            strings.TrimSpace(item.State),
		BaseSHA:          strings.TrimSpace(item.BaseSHA),
		HeadSHA:          strings.TrimSpace(item.HeadSHA),
	}
}

func firstProjectPatchQueueProof(proofs ...*sqlite.ProjectPatchQueueDurabilityProof) *sqlite.ProjectPatchQueueDurabilityProof {
	if len(proofs) == 0 {
		return nil
	}
	return proofs[0]
}

func projectPatchQueueProofIsDurable(proof *sqlite.ProjectPatchQueueDurabilityProof) bool {
	if proof == nil {
		return false
	}
	return proof.Durable &&
		strings.EqualFold(strings.TrimSpace(proof.State), "ok") &&
		sqlite.VerifyProjectPatchQueueDurabilityProof(*proof) == nil
}

func repoMutationWorktreeIdentityFromCandidate(ctx context.Context, candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.WorktreeIdentityEvidence {
	item := candidate.QueueItem
	branch := candidate.Branch
	checkout := candidate.Checkout
	evidence := repoauthority.WorktreeIdentityEvidence{
		RepoID:     strings.TrimSpace(item.RepoID),
		CheckoutID: strings.TrimSpace(checkout.CheckoutID),
		BranchID:   strings.TrimSpace(item.BranchID),
		BranchName: firstNonEmpty(branch.BranchName, checkout.BranchName),
		MachineID:  strings.TrimSpace(checkout.MachineID),
		LocalPath:  strings.TrimSpace(checkout.LocalPath),
		BaseSHA:    strings.TrimSpace(item.BaseSHA),
		HeadSHA:    strings.TrimSpace(item.HeadSHA),
	}
	readback := repoMutationReadGitWorktree(ctx, evidence.LocalPath, evidence.BranchName, evidence.HeadSHA)
	evidence.ReadbackState = readback.State
	evidence.ReadbackError = readback.Error
	evidence.ObservedWorktreeRoot = readback.WorktreeRoot
	evidence.ObservedBranchName = readback.BranchName
	evidence.ObservedHeadSHA = readback.HeadSHA
	evidence.ObservedDirtyState = readback.DirtyState
	return evidence
}

func repoMutationTargetWorktreeIdentityFromCandidate(ctx context.Context, candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.WorktreeIdentityEvidence {
	item := candidate.QueueItem
	target := candidate.TargetCheckout
	if strings.TrimSpace(target.CheckoutID) == "" {
		return repoauthority.WorktreeIdentityEvidence{}
	}
	targetBranch := firstNonEmpty(target.BranchName, candidate.Repository.IntegrationBranch, candidate.Repository.DefaultBranch, item.BaseRef)
	evidence := repoauthority.WorktreeIdentityEvidence{
		RepoID:     strings.TrimSpace(item.RepoID),
		CheckoutID: strings.TrimSpace(target.CheckoutID),
		BranchID:   strings.TrimSpace(target.CheckoutID),
		BranchName: targetBranch,
		MachineID:  strings.TrimSpace(target.MachineID),
		LocalPath:  strings.TrimSpace(target.LocalPath),
		BaseSHA:    strings.TrimSpace(item.BaseSHA),
		HeadSHA:    strings.TrimSpace(item.BaseSHA),
	}
	readback := repoMutationReadGitWorktree(ctx, evidence.LocalPath, evidence.BranchName, evidence.HeadSHA)
	evidence.ReadbackState = readback.State
	evidence.ReadbackError = readback.Error
	evidence.ObservedWorktreeRoot = readback.WorktreeRoot
	evidence.ObservedBranchName = readback.BranchName
	evidence.ObservedHeadSHA = readback.HeadSHA
	evidence.ObservedDirtyState = readback.DirtyState
	return evidence
}

type repoMutationGitWorktreeReadback struct {
	State        string
	Error        string
	WorktreeRoot string
	BranchName   string
	HeadSHA      string
	DirtyState   string
}

func repoMutationReadGitWorktree(ctx context.Context, localPath string, expectedBranch string, expectedHead string) repoMutationGitWorktreeReadback {
	localPath = strings.TrimSpace(localPath)
	if localPath == "" {
		return repoMutationGitWorktreeReadback{State: "missing", Error: "local_path is empty", DirtyState: "unknown"}
	}
	info, err := os.Stat(localPath)
	if err != nil {
		return repoMutationGitWorktreeReadback{State: "missing", Error: err.Error(), DirtyState: "unknown"}
	}
	if !info.IsDir() {
		return repoMutationGitWorktreeReadback{State: "missing", Error: "local_path is not a directory", DirtyState: "unknown"}
	}
	if _, err := exec.LookPath("git"); err != nil {
		return repoMutationGitWorktreeReadback{State: "error", Error: "git executable not found", DirtyState: "unknown"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	root, err := repoMutationGitOutput(readCtx, localPath, "rev-parse", "--show-toplevel")
	if err != nil {
		return repoMutationGitWorktreeReadback{State: "not_git", Error: err.Error(), DirtyState: "unknown"}
	}
	branchName, err := repoMutationGitOutput(readCtx, localPath, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return repoMutationGitWorktreeReadback{State: "error", Error: err.Error(), WorktreeRoot: root, DirtyState: "unknown"}
	}
	headSHA, err := repoMutationGitOutput(readCtx, localPath, "rev-parse", "HEAD")
	if err != nil {
		return repoMutationGitWorktreeReadback{State: "error", Error: err.Error(), WorktreeRoot: root, BranchName: branchName, DirtyState: "unknown"}
	}
	status, err := repoMutationGitOutput(readCtx, localPath, "status", "--porcelain")
	if err != nil {
		return repoMutationGitWorktreeReadback{State: "error", Error: err.Error(), WorktreeRoot: root, BranchName: branchName, HeadSHA: headSHA, DirtyState: "unknown"}
	}
	dirtyState := "clean"
	if strings.TrimSpace(status) != "" {
		dirtyState = "dirty"
	}

	readback := repoMutationGitWorktreeReadback{
		State:        "ok",
		WorktreeRoot: root,
		BranchName:   branchName,
		HeadSHA:      headSHA,
		DirtyState:   dirtyState,
	}
	switch {
	case !pathsEqual(root, localPath):
		readback.State = "mismatch"
		readback.Error = "git worktree root does not match registered local_path"
	case strings.TrimSpace(expectedBranch) != "" && strings.TrimSpace(branchName) != strings.TrimSpace(expectedBranch):
		readback.State = "mismatch"
		readback.Error = "git branch does not match registered branch"
	case strings.TrimSpace(expectedHead) != "" && strings.TrimSpace(headSHA) != strings.TrimSpace(expectedHead):
		readback.State = "mismatch"
		readback.Error = "git HEAD does not match registered head_sha"
	case dirtyState != "clean":
		readback.State = "dirty"
		readback.Error = "git worktree has uncommitted changes"
	}
	return readback
}

func repoMutationGitOutput(ctx context.Context, localPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", localPath}, args...)...)
	raw, err := cmd.CombinedOutput()
	output := strings.TrimSpace(string(raw))
	if err != nil {
		if output == "" {
			output = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), output)
	}
	return output, nil
}

func repoMutationContextFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.Context {
	item := candidate.QueueItem
	branch := candidate.Branch
	checkout := candidate.Checkout
	operationID := ""
	operationKind := ""
	if sqlite.ProjectPatchQueueOperationBindingReady(item) {
		operationID = strings.TrimSpace(item.OperationID)
		operationKind = strings.TrimSpace(item.OperationKind)
	}
	agentID := firstNonEmpty(item.AgentID, item.SubmittedBy, branch.AgentID, checkout.AgentID, item.ClaimedBy)
	taskID := firstNonEmpty(item.TaskID, branch.ActiveTaskID, checkout.ActiveTaskID)
	principalType := firstNonEmpty(item.PrincipalType, "agent")
	principalID := firstNonEmpty(item.PrincipalID, agentID)
	return repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: strings.TrimSpace(item.WorkspaceID),
		TaskID:      taskID,
		SessionID:   strings.TrimSpace(item.SessionID),
		RunID:       strings.TrimSpace(item.RunID),
		AgentID:     agentID,
		Principal: repoauthority.PrincipalRef{
			Type: principalType,
			ID:   principalID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     strings.TrimSpace(item.CapabilitySnapshotID),
			Schema: strings.TrimSpace(item.CapabilitySnapshotSchema),
		},
		RepoRoot: firstNonEmpty(item.RepoRoot, checkout.LocalPath),
		Base: repoauthority.BaseIdentity{
			Ref:        firstNonEmpty(item.BaseRef, branch.BaseBranch, checkout.BaseBranch),
			TreeHash:   strings.TrimSpace(item.BaseTreeHash),
			FileHashes: cloneStringMap(item.BaseFileHashes),
		},
		Pathset: append([]string(nil), item.Pathset...),
		Lease: repoauthority.LeaseRef{
			ID:   strings.TrimSpace(item.RepoLeaseID),
			Term: item.LeaseTerm,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: strings.TrimSpace(item.QueueID),
			ItemID:  strings.TrimSpace(item.ItemID),
		},
		Operation: repoauthority.OperationRef{
			ID:   operationID,
			Kind: operationKind,
		},
	}
}

func repoMutationPatchQueueItemFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchQueueItem {
	item := candidate.QueueItem
	branch := candidate.Branch
	checkout := candidate.Checkout
	operationID := ""
	operationKind := ""
	if sqlite.ProjectPatchQueueOperationBindingReady(item) {
		operationID = strings.TrimSpace(item.OperationID)
		operationKind = strings.TrimSpace(item.OperationKind)
	}
	state := strings.ToLower(strings.TrimSpace(item.State))
	var casResult repoauthority.CASPatchApplyResult
	var casPatchDigest string
	var casEvaluationDigest string
	var testEvidence repoauthority.PatchQueueTestEvidence
	var testEvidenceDigest string
	var rollbackEvidence repoauthority.PatchQueueRollback
	var rollbackEvidenceDigest string
	var reviewerAdvisory repoauthority.PatchQueueReviewerAdvisory
	var reviewerAdvisoryDigest string
	var operatorEnablement repoauthority.PatchQueueOperatorEnablement
	var operatorEnablementDigest string
	if sqlite.ProjectPatchQueueCASEvidenceReady(item) {
		state = repoauthority.PatchQueueStateApplied
		casResult = item.CASResult
		casPatchDigest = strings.TrimSpace(item.CASPatchDigest)
		casEvaluationDigest = strings.TrimSpace(item.CASEvaluationDigest)
		testEvidence = item.CASTestEvidence
		testEvidenceDigest = strings.TrimSpace(item.CASTestEvidenceDigest)
	}
	if sqlite.ProjectPatchQueueRollbackEvidenceReady(item) {
		rollbackEvidence = item.RollbackEvidence
		rollbackEvidenceDigest = strings.TrimSpace(item.RollbackEvidenceDigest)
	}
	if sqlite.ProjectPatchQueueReviewerAdvisoryReady(item) {
		reviewerAdvisory = item.ReviewerAdvisory
		reviewerAdvisoryDigest = strings.TrimSpace(item.ReviewerAdvisoryDigest)
	}
	if sqlite.ProjectPatchQueueOperatorEnablementReady(item) {
		operatorEnablement = item.OperatorEnablement
		operatorEnablementDigest = strings.TrimSpace(item.OperatorEnablementDigest)
	}
	agentID := firstNonEmpty(item.AgentID, item.SubmittedBy, branch.AgentID, checkout.AgentID, item.ClaimedBy)
	taskID := firstNonEmpty(item.TaskID, branch.ActiveTaskID, checkout.ActiveTaskID)
	principalType := firstNonEmpty(item.PrincipalType, "agent")
	principalID := firstNonEmpty(item.PrincipalID, agentID)
	return repoauthority.PatchQueueItem{
		Schema:                   repoauthority.PatchQueueItemSchemaVersion,
		ID:                       strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID),
		QueueID:                  strings.TrimSpace(item.QueueID),
		ItemID:                   strings.TrimSpace(item.ItemID),
		ReviewDocKey:             strings.TrimSpace(item.ReviewDocKey),
		State:                    state,
		Attempt:                  item.Attempt,
		MaxAttempts:              item.MaxAttempts,
		NextRetryAt:              strings.TrimSpace(item.NextRetryAt),
		DeadLetteredAt:           strings.TrimSpace(item.DeadLetteredAt),
		Pathset:                  append([]string(nil), item.Pathset...),
		WorkspaceID:              strings.TrimSpace(item.WorkspaceID),
		ProjectID:                strings.TrimSpace(item.ProjectID),
		TaskID:                   taskID,
		SessionID:                strings.TrimSpace(item.SessionID),
		RunID:                    strings.TrimSpace(item.RunID),
		AgentID:                  agentID,
		PrincipalType:            principalType,
		PrincipalID:              principalID,
		CapabilitySnapshotID:     strings.TrimSpace(item.CapabilitySnapshotID),
		CapabilitySnapshotSchema: strings.TrimSpace(item.CapabilitySnapshotSchema),
		BaseRef:                  firstNonEmpty(item.BaseRef, branch.BaseBranch, checkout.BaseBranch),
		BaseTreeHash:             strings.TrimSpace(item.BaseTreeHash),
		ContextDigest:            strings.TrimSpace(item.ContextDigest),
		RepoLeaseID:              strings.TrimSpace(item.RepoLeaseID),
		LeaseTerm:                item.LeaseTerm,
		CASResult:                casResult,
		CASPatchDigest:           casPatchDigest,
		CASEvaluationDigest:      casEvaluationDigest,
		TestEvidence:             testEvidence,
		TestEvidenceDigest:       testEvidenceDigest,
		RollbackEvidence:         rollbackEvidence,
		RollbackEvidenceDigest:   rollbackEvidenceDigest,
		ReviewerAdvisory:         reviewerAdvisory,
		ReviewerAdvisoryDigest:   reviewerAdvisoryDigest,
		OperatorEnablement:       operatorEnablement,
		OperatorEnablementDigest: operatorEnablementDigest,
		OperationID:              operationID,
		OperationKind:            operationKind,
		CreatedAt:                strings.TrimSpace(item.CreatedAt),
		UpdatedAt:                strings.TrimSpace(item.UpdatedAt),
	}
}

func repoMutationRollbackEvidenceFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchQueueRollback {
	item := candidate.QueueItem
	if sqlite.ProjectPatchQueueRollbackEvidenceReady(item) {
		return item.RollbackEvidence
	}
	return repoauthority.PatchQueueRollback{}
}

func repoMutationReviewerAdvisoryFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchQueueReviewerAdvisory {
	item := candidate.QueueItem
	if sqlite.ProjectPatchQueueReviewerAdvisoryReady(item) {
		return item.ReviewerAdvisory
	}
	return repoauthority.PatchQueueReviewerAdvisory{}
}

func repoMutationOperatorEnablementFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchQueueOperatorEnablement {
	item := candidate.QueueItem
	if sqlite.ProjectPatchQueueOperatorEnablementReady(item) {
		return item.OperatorEnablement
	}
	return repoauthority.PatchQueueOperatorEnablement{}
}

func repoMutationPatchMaterializationFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchMaterialization {
	item := candidate.QueueItem
	if sqlite.ProjectPatchQueueMaterializationReady(item) {
		return item.Materialization
	}
	return repoauthority.PatchMaterialization{}
}

func repoMutationPatchMaterializationAuthorityProofFromCandidate(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchMaterializationAuthorityProof {
	item := candidate.QueueItem
	if sqlite.ProjectPatchQueueMaterializationReady(item) {
		return item.MaterializationAuthorityProof
	}
	return repoauthority.PatchMaterializationAuthorityProof{}
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func applyProjectPatchQueueDurabilityHealth(payload *serviceHealthPayload, proof sqlite.ProjectPatchQueueDurabilityProof) {
	if payload == nil {
		return
	}
	state := strings.ToLower(strings.TrimSpace(proof.State))
	if state == "" || state == "ok" {
		return
	}
	message := strings.TrimSpace(proof.Message)
	if message == "" {
		message = "project patch queue durability state is " + state
	}
	payload.Status = "degraded"
	payload.Semantics.Degraded = appendDiagnosticMessage(payload.Semantics.Degraded, DiagnosticSignal{
		State:   "degraded",
		Message: message,
	})
	if strings.EqualFold(strings.TrimSpace(payload.Semantics.DeploymentReadiness.State), "ok") ||
		strings.TrimSpace(payload.Semantics.DeploymentReadiness.State) == "" {
		payload.Semantics.DeploymentReadiness = DiagnosticSignal{State: "degraded", Message: message}
		return
	}
	if strings.EqualFold(strings.TrimSpace(payload.Semantics.DeploymentReadiness.State), "degraded") {
		payload.Semantics.DeploymentReadiness = appendDiagnosticMessage(payload.Semantics.DeploymentReadiness, DiagnosticSignal{
			State:   "degraded",
			Message: message,
		})
	}
}

func applyProjectPatchQueueActuatorHealth(payload *serviceHealthPayload, snapshot sqlite.ProjectPatchQueueActuatorHealthSnapshot) {
	if payload == nil {
		return
	}
	state := strings.ToLower(strings.TrimSpace(snapshot.State))
	if state == "" || state == "ok" || state == "unsupported" {
		return
	}
	message := strings.TrimSpace(snapshot.Message)
	if message == "" {
		message = "repo mutation actuator journal state is " + state
	}
	payload.Status = "degraded"
	payload.Semantics.Degraded = appendDiagnosticMessage(payload.Semantics.Degraded, DiagnosticSignal{
		State:   "degraded",
		Message: message,
	})
	if strings.EqualFold(strings.TrimSpace(payload.Semantics.DeploymentReadiness.State), "ok") ||
		strings.TrimSpace(payload.Semantics.DeploymentReadiness.State) == "" {
		payload.Semantics.DeploymentReadiness = DiagnosticSignal{State: "degraded", Message: message}
		return
	}
	if strings.EqualFold(strings.TrimSpace(payload.Semantics.DeploymentReadiness.State), "degraded") {
		payload.Semantics.DeploymentReadiness = appendDiagnosticMessage(payload.Semantics.DeploymentReadiness, DiagnosticSignal{
			State:   "degraded",
			Message: message,
		})
	}
}

func appendDiagnosticMessage(current DiagnosticSignal, next DiagnosticSignal) DiagnosticSignal {
	nextState := strings.TrimSpace(next.State)
	nextMessage := strings.TrimSpace(next.Message)
	if nextState == "" {
		nextState = strings.TrimSpace(current.State)
	}
	if nextMessage == "" {
		return DiagnosticSignal{State: nextState, Message: strings.TrimSpace(current.Message)}
	}
	currentMessage := strings.TrimSpace(current.Message)
	if currentMessage == "" || strings.EqualFold(currentMessage, "no known degradation") {
		return DiagnosticSignal{State: nextState, Message: nextMessage}
	}
	if strings.Contains(currentMessage, nextMessage) {
		return DiagnosticSignal{State: nextState, Message: currentMessage}
	}
	return DiagnosticSignal{State: nextState, Message: currentMessage + "; " + nextMessage}
}

func budgetLedgerDegradedReason(snapshot sqlite.BudgetLedgerHealthSnapshot) string {
	status := strings.ToLower(strings.TrimSpace(snapshot.Status))
	if status == "" {
		status = "unknown"
	}
	message := strings.TrimSpace(snapshot.Message)
	if message != "" {
		return fmt.Sprintf("budget ledger status is %s: %s", status, message)
	}
	return fmt.Sprintf(
		"budget ledger status is %s (exhausted_accounts=%d stale_open_reservations=%d overspent_accounts=%d)",
		status,
		snapshot.ExhaustedAccountCount,
		snapshot.StaleOpenReservationCount,
		snapshot.OverspentAccountCount,
	)
}

func collectPromptAuthorityScopeDiagnostics() PromptAuthorityScopeDiagnostics {
	return PromptAuthorityScopeDiagnostics{
		State:    "ok",
		Contract: promptAuthorityScopeContract,
		Message:  "first-stable prompt authority scope is explicitly bounded",
		Surfaces: []PromptAuthoritySurfaceBoundary{
			{
				Surface:                               "manager.local_inspect",
				Decision:                              "excluded_read_only_non_daemon",
				AuthorityBoundary:                     "manager_process_read_only_inspect",
				PromptCompilerStatus:                  "manager_mediated_local_inspect_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_read_only_non_daemon",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R1.4.md",
					"tasks/rhizome-remediation-evidence/R3.3.md",
				},
			},
			{
				Surface:                               "manager.inline_local_tui_chat",
				Decision:                              "excluded_compatibility_non_daemon",
				AuthorityBoundary:                     "manager_process_compatibility_chat",
				PromptCompilerStatus:                  "manager_mediated_local_inspect_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_compatibility_non_daemon",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "manager.live_attach.runtime_control",
				Decision:                              "excluded_operator_runtime_control_not_c2_1_convergence",
				AuthorityBoundary:                     "operator_runtime_control_not_daemon_prompt_compiler_evidence",
				PromptCompilerStatus:                  "operator_runtime_control_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_operator_runtime_control_not_c2_1_convergence",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "manager.live_attach.model_ask",
				Decision:                              "request_carrier_only",
				AuthorityBoundary:                     "agent_request_carrier_only_no_separate_daemon_local_control_claim",
				PromptCompilerStatus:                  "request_carrier_bounded_by_agent_request_evidence",
				C21Convergence:                        "request_carrier_only",
				DeploymentEvidence:                    "accepted_only_for_agent_request_carrier_not_local_control_mutation",
				FirstDeploymentPreflight:              "carrier_only_no_separate_daemon_authority",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/C2.1.md",
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "manager.web_runtime_control",
				Decision:                              "excluded_operator_runtime_control_not_c2_1_convergence",
				AuthorityBoundary:                     "operator_web_control_not_daemon_prompt_compiler_evidence",
				PromptCompilerStatus:                  "operator_runtime_control_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_operator_runtime_control_not_c2_1_convergence",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "manager.process_lifecycle_control",
				Decision:                              "excluded_process_supervision_not_prompt_convergence",
				AuthorityBoundary:                     "manager_process_supervision_not_daemon_prompt_compiler_evidence",
				PromptCompilerStatus:                  "manager_process_control_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_process_supervision_not_prompt_convergence",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "internal_agent.tension_lifecycle_update",
				Decision:                              "excluded_legacy_native_direct_tool",
				AuthorityBoundary:                     "legacy_internal_agent_tool_not_first_stable_daemon_surface",
				PromptCompilerStatus:                  "legacy_native_tool_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_until_migrated_to_prompt_envelope",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
				},
			},
			{
				Surface:                               "internal_agent.memory_write",
				Decision:                              "excluded_legacy_native_direct_tool",
				AuthorityBoundary:                     "direct_sqlite_internal_agent_tool_not_first_stable_daemon_surface",
				PromptCompilerStatus:                  "legacy_native_tool_non_converged",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_until_migrated_to_workspace_memory_rpc_envelope",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.1.md",
					"tasks/rhizome-remediation-evidence/C2.1.md",
				},
			},
			{
				Surface:                               "internal_living.memory_write",
				Decision:                              "covered_when_routed_through_workspace_memory_rpc",
				AuthorityBoundary:                     "server_rpc_workspace_memory_envelope_required",
				PromptCompilerStatus:                  "covered_by_prompt_context_envelope_when_eventful",
				C21Convergence:                        "covered_when_workspace_memory_rpc_event_present",
				DeploymentEvidence:                    "accepted_only_with_workspace_memory_prompt_context_envelope",
				FirstDeploymentPreflight:              "covered_rpc_path_only",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: false,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/C2.1.md",
				},
			},
			{
				Surface:                               "executor.node.report_progress",
				Decision:                              "excluded_log_only_non_authority",
				AuthorityBoundary:                     "log_only_non_authority_bearing_callback",
				PromptCompilerStatus:                  "executor_progress_callback_exclusion.v1",
				C21Convergence:                        promptAuthorityStatusExcluded,
				DeploymentEvidence:                    promptAuthorityDeploymentEvidenceRejected,
				FirstDeploymentPreflight:              "excluded_log_only_non_authority",
				AcceptedAsDaemonConvergence:           false,
				RequiresPromptEnvelopeBeforePromotion: true,
				Evidence: []string{
					"tasks/rhizome-remediation-evidence/R3.5.md",
				},
			},
		},
	}
}

func runtimeWorkGateFallbackCanConsumeWork(diag *RuntimeWorkGateDiagnostics) bool {
	if diag == nil {
		return false
	}
	fallback := diag.BootstrapWorkFallback
	if fallback.CanConsumeWork {
		return true
	}
	posture := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		fallback.Posture,
		fallback.GateState,
		fallback.Scope,
	}, " ")))
	return strings.Contains(posture, "enabled") || strings.Contains(posture, "open")
}

func degradedMessage(reasons []string) string {
	filtered := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		if strings.TrimSpace(reason) != "" {
			filtered = append(filtered, strings.TrimSpace(reason))
		}
	}
	if len(filtered) == 0 {
		return "one or more loops or metrics are degraded"
	}
	return strings.Join(filtered, "; ")
}

func runtimeRevisionDrifts(runtimeInfo app.RuntimeBuildInfo, checkout app.GitCheckoutInfo) bool {
	runtimeRevision := strings.TrimSpace(runtimeInfo.VCSRevision)
	checkoutHead := strings.TrimSpace(checkout.Head)
	if runtimeRevision == "" || checkoutHead == "" {
		return false
	}
	if strings.TrimSpace(checkout.Error) != "" {
		return false
	}
	return runtimeRevision != checkoutHead
}

func evaluateExtendedReadiness(
	registry *ReadinessRegistry,
	projectionLag sqlite.MemoryProjectionLagSnapshot,
	operatorQueueLag DiagnosticSignal,
	reviewerScarcity DiagnosticSignal,
	reviewerScarcityHealth sqlite.ReviewerScarcityHealthSnapshot,
	stuckAgents DiagnosticSignal,
	noProgress DiagnosticSignal,
) ExtendedReadiness {
	if strings.TrimSpace(operatorQueueLag.State) == "" {
		operatorQueueLag = DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"}
	}
	if strings.TrimSpace(reviewerScarcity.State) == "" {
		reviewerScarcity = DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"}
	}
	if strings.TrimSpace(stuckAgents.State) == "" {
		stuckAgents = DiagnosticSignal{State: "unsupported", Message: "stuck agent health not collected"}
	}
	ext := ExtendedReadiness{
		InvalidationLag:        DiagnosticSignal{State: "unsupported", Message: "requires global O(1) index, currently missing"},
		OperatorQueueLag:       operatorQueueLag,
		ReviewerScarcity:       reviewerScarcity,
		ReviewerScarcityHealth: reviewerScarcityHealth,
		StuckAgents:            stuckAgents,
		NoProgress:             noProgress,
		ReplayHealth:           DiagnosticSignal{State: "unsupported", Message: "lacks global cross-workspace aggregation view"},
		ProjectionLag:          projectionLag,
	}
	if ext.ProjectionLag.State == "" {
		ext.ProjectionLag = sqlite.MemoryProjectionLagSnapshot{
			State:   "unsupported",
			Message: "memory projection lag requires outbox-backed store visibility",
		}
	}
	if strings.TrimSpace(ext.NoProgress.State) == "" {
		ext.NoProgress = DiagnosticSignal{}
	}

	ext.MotifLifecycle = DiagnosticSignal{State: "missing"}
	if registry != nil {
		firehose := registry.Get("firehose")
		if firehose != nil && firehose.State == LoopRunning {
			ext.MotifLifecycle = DiagnosticSignal{State: "ok", Message: "synchronous within firehose loop"}
		} else if firehose != nil {
			ext.MotifLifecycle = DiagnosticSignal{State: "degraded", Message: fmt.Sprintf("firehose is %s", firehose.State)}
		}
	}
	return ext
}

func reviewerScarcityDiagnosticSignal(snapshot sqlite.ReviewerScarcityHealthSnapshot) DiagnosticSignal {
	state := strings.TrimSpace(snapshot.State)
	message := strings.TrimSpace(snapshot.Message)
	if state == "" {
		return DiagnosticSignal{}
	}
	if strings.EqualFold(state, "degraded") &&
		snapshot.SaturatedWorkspaceCount == 0 &&
		snapshot.ScarceWorkspaceCount == 0 &&
		snapshot.UnknownWorkspaceCount > 0 {
		return DiagnosticSignal{
			State:   "partial",
			Message: firstNonEmpty(message, fmt.Sprintf("reviewer scarcity health is partial: unknown_workspaces=%d", snapshot.UnknownWorkspaceCount)),
		}
	}
	return DiagnosticSignal{State: state, Message: message}
}

func collectProjectionLagSnapshot(store *sqlite.Store) sqlite.MemoryProjectionLagSnapshot {
	if store == nil {
		return sqlite.MemoryProjectionLagSnapshot{
			State:   "unsupported",
			Message: "sqlite store unavailable",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snapshot, err := store.MemoryProjectionLagSnapshot(ctx)
	if err != nil {
		return sqlite.MemoryProjectionLagSnapshot{
			State:   "unsupported",
			Message: err.Error(),
			Error:   err.Error(),
		}
	}
	return snapshot
}

func collectBudgetLedgerHealthSnapshot(store *sqlite.Store) *sqlite.BudgetLedgerHealthSnapshot {
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	if store == nil {
		return &sqlite.BudgetLedgerHealthSnapshot{
			Contract:    sqlite.BudgetLedgerHealthContract,
			ReferenceAt: referenceAt,
			Status:      "error",
			Message:     "sqlite store unavailable",
			Error:       "sqlite store unavailable",
			Reasons:     []string{"collection_error"},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snapshot, err := store.GetBudgetLedgerHealthSnapshot(ctx, serveBudgetLedgerStaleAfter)
	if err != nil {
		return &sqlite.BudgetLedgerHealthSnapshot{
			Contract:    sqlite.BudgetLedgerHealthContract,
			ReferenceAt: referenceAt,
			Status:      "error",
			Message:     "budget ledger health collection failed",
			Error:       err.Error(),
			Reasons:     []string{"collection_error"},
		}
	}
	return &snapshot
}

func collectDurableRuntimeSnapshot(dbPath string) *durableRuntimeSnapshot {
	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	dbPath = strings.TrimSpace(dbPath)
	snapshot := &durableRuntimeSnapshot{
		State:       "unsupported",
		Message:     "durable runtime readback is not available",
		ReferenceAt: referenceAt,
	}
	if dbPath == "" {
		snapshot.Message = "durable runtime readback skipped because db path is empty"
		return snapshot
	}

	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			snapshot.Message = "durable runtime readback skipped because db file is missing"
			return snapshot
		}
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("stat durable runtime db: %v", err)
		return snapshot
	}

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("open durable runtime db: %v", err)
		return snapshot
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	detail, skippedEmptyRun, err := latestDurableRuntimeDetail(ctx, store)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			snapshot.Message = "durable runtime readback skipped because no execution runs exist yet"
			return snapshot
		}
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			snapshot.Message = "durable runtime readback skipped because execution tables are unavailable"
			return snapshot
		}
		snapshot.State = "error"
		snapshot.Message = fmt.Sprintf("query latest durable execution run: %v", err)
		return snapshot
	}
	if detail == nil {
		snapshot.State = "missing"
		snapshot.Message = "durable runtime readback found execution runs but no step evidence"
		if skippedEmptyRun != nil {
			snapshot.WorkspaceID = skippedEmptyRun.WorkspaceID
			snapshot.RunID = skippedEmptyRun.RunID
			snapshot.Message = fmt.Sprintf("latest durable execution run %s/%s has no step evidence and no earlier checkpoint exists", skippedEmptyRun.WorkspaceID, skippedEmptyRun.RunID)
		}
		return snapshot
	}

	snapshot.WorkspaceID = detail.Run.WorkspaceID
	snapshot.RunID = detail.Run.RunID
	snapshot.SessionID = detail.Run.SessionID
	snapshot.TaskID = detail.Run.TaskID
	snapshot.AgentID = detail.Run.AgentID
	snapshot.RunStatus = detail.Run.Status
	snapshot.RunOutcome = detail.Run.Outcome
	snapshot.RunTitle = detail.Run.Title
	snapshot.RunSummary = detail.Run.Summary
	snapshot.Evidence = make([]string, 0, 8)

	if strings.TrimSpace(snapshot.SessionID) == "" {
		snapshot.State = "missing"
		snapshot.Message = "latest durable execution run is missing session evidence"
		return snapshot
	}

	step := detail.Steps[0]
	for _, candidate := range detail.Steps[1:] {
		if durableRuntimeStepMoreRecent(candidate, step) {
			step = candidate
		}
	}
	snapshot.StepID = step.StepID
	snapshot.StepPhase = step.Phase
	snapshot.StepTitle = step.Title
	snapshot.StepStatus = step.Status
	snapshot.StepSummary = step.Summary
	snapshot.Evidence = append(snapshot.Evidence, step.Evidence...)
	snapshot.Progress = durableRuntimeProgressToken(step.VerificationJSON)
	if snapshot.Progress == "" {
		snapshot.Progress = firstNonEmpty(step.Title, step.Status)
	}

	sessionState, err := store.GetAgentSessionState(ctx, snapshot.WorkspaceID, snapshot.SessionID)
	if err != nil {
		snapshot.State = "missing"
		snapshot.Message = fmt.Sprintf("latest durable session evidence is missing: %v", err)
		return snapshot
	}
	snapshot.SessionStatus = sessionState.Status
	snapshot.SessionSummary = sessionState.Summary
	snapshot.SessionUpdatedAt = sessionState.UpdatedAt

	issues := make([]string, 0, 4)
	if sessionState.TaskID != "" && snapshot.TaskID != "" && sessionState.TaskID != snapshot.TaskID {
		issues = append(issues, fmt.Sprintf("session task %s differs from run task %s", sessionState.TaskID, snapshot.TaskID))
	}
	if sessionState.AgentID != "" && snapshot.AgentID != "" && sessionState.AgentID != snapshot.AgentID {
		issues = append(issues, fmt.Sprintf("session agent %s differs from run agent %s", sessionState.AgentID, snapshot.AgentID))
	}

	if operation := durableRuntimeOperationEvidenceFromRun(detail.Run); operation != nil {
		snapshot.OperationID = operation.OperationID
		snapshot.OperationName = operation.OperationName
		snapshot.OperationKind = operation.OperationKind
		snapshot.OperationStatus = operation.OperationStatus
		snapshot.OperationUpdatedAt = operation.OperationUpdatedAt
		snapshot.OperationBindingRunID = operation.BindingRunID
		snapshot.OperationBindingSessionID = operation.BindingSessionID
		snapshot.OperationBindingTaskID = operation.BindingTaskID
		snapshot.OperationBindingAgentID = operation.BindingAgentID
		if operation.BindingRunID != "" && operation.BindingRunID != snapshot.RunID {
			issues = append(issues, fmt.Sprintf("operation binding run %s differs from run %s", operation.BindingRunID, snapshot.RunID))
		}
		if operation.BindingSessionID != "" && operation.BindingSessionID != snapshot.SessionID {
			issues = append(issues, fmt.Sprintf("operation binding session %s differs from run session %s", operation.BindingSessionID, snapshot.SessionID))
		}
		if operation.BindingTaskID != "" && snapshot.TaskID != "" && operation.BindingTaskID != snapshot.TaskID {
			issues = append(issues, fmt.Sprintf("operation binding task %s differs from run task %s", operation.BindingTaskID, snapshot.TaskID))
		}
		if operation.BindingAgentID != "" && snapshot.AgentID != "" && operation.BindingAgentID != snapshot.AgentID {
			issues = append(issues, fmt.Sprintf("operation binding agent %s differs from run agent %s", operation.BindingAgentID, snapshot.AgentID))
		}
		if snapshot.Progress == "" {
			snapshot.Progress = firstNonEmpty(operation.OperationStatus, operation.OperationName, operation.OperationID)
		}
	}
	promptCompiler := durableRuntimePromptCompilerSnapshotWithSnapshotReadback(detail)
	snapshot.PromptCompiler = &promptCompiler

	if len(issues) > 0 {
		snapshot.State = "mismatch"
		snapshot.Issues = issues
		snapshot.Message = "durable runtime evidence mismatch: " + strings.Join(issues, "; ")
		return snapshot
	}

	snapshot.State = "ok"
	snapshot.Message = fmt.Sprintf(
		"durable runtime restored from run=%s session=%s phase=%s progress=%s",
		snapshot.RunID,
		snapshot.SessionID,
		snapshot.StepPhase,
		firstNonEmpty(snapshot.Progress, "unknown"),
	)
	return snapshot
}

type durableRuntimeRunRef struct {
	WorkspaceID string
	RunID       string
}

func latestDurableRuntimeDetail(ctx context.Context, store *sqlite.Store) (*sqlite.ExecutionRunDetail, *durableRuntimeRunRef, error) {
	latestRun, err := latestDurableRuntimeRunRef(ctx, store, false)
	if err != nil {
		return nil, nil, err
	}

	steppedRun, err := latestDurableRuntimeRunRef(ctx, store, true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, latestRun, nil
		}
		return nil, latestRun, err
	}

	detail, err := store.GetExecutionRun(ctx, steppedRun.WorkspaceID, steppedRun.RunID)
	if err != nil {
		return nil, latestRun, fmt.Errorf("load durable execution run %s/%s: %w", steppedRun.WorkspaceID, steppedRun.RunID, err)
	}
	if len(detail.Steps) == 0 {
		return nil, latestRun, nil
	}
	return &detail, latestRun, nil
}

func latestDurableRuntimeRunRef(ctx context.Context, store *sqlite.Store, requireStepEvidence bool) (*durableRuntimeRunRef, error) {
	query := `
		SELECT r.workspace_id, r.run_id
		  FROM execution_runs r
		  JOIN workspaces w ON w.workspace_id = r.workspace_id
		 WHERE w.status = ?
		   AND (
		       COALESCE(TRIM(r.session_id), '') <> ''
		       OR COALESCE(TRIM(r.task_id), '') <> ''
		   )
		   AND UPPER(TRIM(COALESCE(r.status, ''))) NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'TIMED_OUT', 'SKIPPED')`
	if requireStepEvidence {
		query += `
		   AND EXISTS (
		       SELECT 1
		         FROM execution_steps s
		        WHERE s.workspace_id = r.workspace_id
		          AND s.run_id = r.run_id
		   )`
	}
	query += `
		 ORDER BY r.updated_at DESC, r.run_id DESC
		 LIMIT 1`

	var ref durableRuntimeRunRef
	if err := store.DB().QueryRowContext(ctx, query, model.WorkspaceStatusActive).Scan(&ref.WorkspaceID, &ref.RunID); err != nil {
		return nil, err
	}
	return &ref, nil
}

type durableRuntimeOperation struct {
	OperationID        string
	OperationName      string
	OperationKind      string
	OperationStatus    string
	OperationUpdatedAt string
	BindingRunID       string
	BindingSessionID   string
	BindingTaskID      string
	BindingAgentID     string
}

func durableRuntimeOperationEvidenceFromRun(run sqlite.ExecutionRunRecord) *durableRuntimeOperation {
	ledger, ok := run.VerificationJSON["operation_ledger"].(map[string]any)
	if !ok || len(ledger) == 0 {
		return nil
	}
	op := &durableRuntimeOperation{
		OperationID:        durableRuntimeMapString(ledger, "operation_id"),
		OperationName:      durableRuntimeMapString(ledger, "operation_name"),
		OperationKind:      durableRuntimeMapString(ledger, "operation_kind"),
		OperationStatus:    firstNonEmpty(durableRuntimeMapString(ledger, "status"), run.Status),
		OperationUpdatedAt: firstNonEmpty(durableRuntimeMapString(ledger, "updated_at"), run.UpdatedAt),
		BindingRunID:       durableRuntimeNestedMapString(ledger, "binding", "run_id"),
		BindingSessionID:   durableRuntimeNestedMapString(ledger, "binding", "session_id"),
		BindingTaskID:      durableRuntimeNestedMapString(ledger, "binding", "task_id"),
		BindingAgentID:     durableRuntimeNestedMapString(ledger, "binding", "agent_id"),
	}
	if op.BindingRunID == "" {
		op.BindingRunID = run.RunID
	}
	if op.BindingTaskID == "" {
		op.BindingTaskID = run.TaskID
	}
	if op.BindingSessionID == "" {
		op.BindingSessionID = run.SessionID
	}
	if op.BindingAgentID == "" {
		op.BindingAgentID = run.AgentID
	}
	return op
}

func durableRuntimePromptCompilerSnapshot(detail *sqlite.ExecutionRunDetail) durablePromptCompilerSnapshot {
	return durableRuntimePromptCompilerSnapshotFromDetail(detail, false)
}

func durableRuntimePromptCompilerSnapshotWithSnapshotReadback(detail *sqlite.ExecutionRunDetail) durablePromptCompilerSnapshot {
	return durableRuntimePromptCompilerSnapshotFromDetail(detail, true)
}

func durableRuntimePromptCompilerSnapshotFromDetail(detail *sqlite.ExecutionRunDetail, requireSnapshotReadback bool) durablePromptCompilerSnapshot {
	snapshot := durablePromptCompilerSnapshot{
		State:   "not_evaluated",
		Message: "daemon prompt compiler convergence proof not present in latest durable run",
	}
	if detail == nil || len(detail.Steps) == 0 {
		return snapshot
	}

	capabilitySnapshotEvidenceSeen := false
	latestCapabilitySnapshotRef := ""
	latestCapabilitySnapshotStepIdx := -1
	proofStepIdx := -1
	for idx, step := range detail.Steps {
		if ref := durableRuntimePromptMarkerEvidenceRef(step.Evidence); ref != "" {
			return durablePromptCompilerSnapshot{
				State:   "mismatch",
				Message: "step evidence contains prompt compiler marker instead of durable ref: " + ref,
				StepID:  step.StepID,
			}
		}
		if ref, issue := durableRuntimeCapabilitySnapshotRefFromStep(step); issue != "" {
			return durablePromptCompilerSnapshot{
				State:   "mismatch",
				Message: issue,
				StepID:  step.StepID,
			}
		} else if ref != "" {
			capabilitySnapshotEvidenceSeen = true
			if latestCapabilitySnapshotStepIdx < 0 || durableRuntimeStepMoreRecent(step, detail.Steps[latestCapabilitySnapshotStepIdx]) {
				latestCapabilitySnapshotStepIdx = idx
				latestCapabilitySnapshotRef = ref
			}
		}
		if raw, exists := step.VerificationJSON["prompt_capability_evidence"]; exists {
			if _, ok := raw.(map[string]any); !ok {
				return durablePromptCompilerSnapshot{
					State:   "mismatch",
					Message: "prompt_capability_evidence is not an object",
					StepID:  step.StepID,
				}
			}
			if proofStepIdx < 0 || durableRuntimeStepMoreRecent(step, detail.Steps[proofStepIdx]) {
				proofStepIdx = idx
			}
		}
		if issue := durableRuntimeLegacyPromptVerificationIssue(step.VerificationJSON); issue != "" {
			return durablePromptCompilerSnapshot{
				State:   "mismatch",
				Message: issue,
				StepID:  step.StepID,
			}
		}
	}
	if proofStepIdx < 0 {
		if capabilitySnapshotEvidenceSeen {
			return durablePromptCompilerSnapshot{
				State:   "missing",
				Message: "capability snapshot evidence is present but daemon prompt compiler proof is missing",
			}
		}
		return snapshot
	}

	step := detail.Steps[proofStepIdx]
	proof := step.VerificationJSON["prompt_capability_evidence"].(map[string]any)
	accepted := durablePromptCompilerSnapshot{
		State:                  "ok",
		Message:                "daemon prompt compiler convergence proof is present",
		StepID:                 step.StepID,
		StepPhase:              step.Phase,
		Contract:               durableRuntimeRawMapString(proof, "contract"),
		CapabilitySnapshotID:   durableRuntimeRawMapString(proof, "capability_snapshot_id"),
		CapabilitySnapshotRef:  durableRuntimeRawMapString(proof, "capability_snapshot_ref"),
		CapabilitySnapshotPath: durableRuntimeRawMapString(proof, "capability_snapshot_path"),
		ProjectionSource:       durableRuntimeRawMapString(proof, "projection_source"),
		ProjectionContract:     durableRuntimeRawMapString(proof, "projection_contract"),
		ProjectionDigest:       durableRuntimeRawMapString(proof, "projection_digest"),
	}
	if err := validateDurableRuntimePromptCompilerProof(proof, step.Evidence); err != nil {
		accepted.State = "mismatch"
		accepted.Message = err.Error()
		return accepted
	}
	if latestCapabilitySnapshotRef != "" && accepted.CapabilitySnapshotRef != latestCapabilitySnapshotRef {
		accepted.State = "mismatch"
		accepted.Message = fmt.Sprintf("latest capability snapshot ref %q differs from prompt compiler proof ref %q", latestCapabilitySnapshotRef, accepted.CapabilitySnapshotRef)
		return accepted
	}
	if requireSnapshotReadback {
		if err := validateDurableRuntimeCapabilitySnapshotReadbackFromDetail(detail, proof, &accepted); err != nil {
			accepted.State = "mismatch"
			accepted.Message = err.Error()
			return accepted
		}
	}
	return accepted
}

func validateDurableRuntimePromptCompilerProof(proof map[string]any, evidence []string) error {
	required := map[string]string{
		"contract":               durablePromptCapabilityEvidenceContract,
		"prompt_compiler_status": durablePromptCompilerStatusConverged,
		"c2_1_convergence":       durablePromptConvergenceAccepted,
		"deployment_evidence":    durablePromptDeploymentEvidenceAccepted,
		"projection_source":      durablePromptProjectionSource,
		"projection_contract":    durablePromptProjectionContract,
		"snapshot_schema":        durablePromptSnapshotSchema,
		"snapshot_kind":          durablePromptSnapshotKind,
		"snapshot_status":        durablePromptSnapshotStatus,
		"prompt_contract":        durablePromptContractID,
	}
	for key, want := range required {
		if got := durableRuntimeRawMapString(proof, key); got != want {
			return fmt.Errorf("daemon prompt compiler proof has invalid %s=%q; expected %q", key, got, want)
		}
	}
	snapshotID := durableRuntimeRawMapString(proof, "capability_snapshot_id")
	if snapshotID == "" {
		return errors.New("daemon prompt compiler proof is missing capability_snapshot_id")
	}
	if !durableRuntimeCanonicalCapabilitySnapshotID(snapshotID) {
		return fmt.Errorf("daemon prompt compiler proof has invalid capability_snapshot_id=%q", snapshotID)
	}
	snapshotRef := durableRuntimeRawMapString(proof, "capability_snapshot_ref")
	if snapshotRef != "capability_snapshot:"+snapshotID {
		return fmt.Errorf("daemon prompt compiler proof has invalid capability_snapshot_ref=%q for capability_snapshot_id=%q", snapshotRef, snapshotID)
	}
	if !durableRuntimeEvidenceRefsContain(evidence, snapshotRef) {
		return fmt.Errorf("daemon prompt compiler proof is missing durable evidence ref %q", snapshotRef)
	}
	digest := durableRuntimeRawMapString(proof, "projection_digest")
	if !durableRuntimeCanonicalSHA256Digest(digest) {
		return fmt.Errorf("daemon prompt compiler proof has invalid projection_digest=%q", digest)
	}
	return nil
}

func validateDurableRuntimeCapabilitySnapshotReadbackFromDetail(detail *sqlite.ExecutionRunDetail, proof map[string]any, accepted *durablePromptCompilerSnapshot) error {
	snapshotID := durableRuntimeRawMapString(proof, "capability_snapshot_id")
	if detail != nil && strings.TrimSpace(snapshotID) != "" {
		if snapshot, ok, err := durableRuntimeEmbeddedCapabilitySnapshot(detail, snapshotID); ok || err != nil {
			if err != nil {
				if accepted != nil {
					accepted.SnapshotReadbackState = "error"
				}
				return err
			}
			if accepted != nil {
				accepted.SnapshotReadbackState = "embedded"
			}
			return validateDurableRuntimeCapabilitySnapshotValue(snapshot, proof, accepted)
		}
	}
	return validateDurableRuntimeCapabilitySnapshotReadback(proof, accepted)
}

func durableRuntimeEmbeddedCapabilitySnapshot(detail *sqlite.ExecutionRunDetail, snapshotID string) (durableRuntimeCapabilitySnapshot, bool, error) {
	snapshotID = strings.TrimSpace(snapshotID)
	if detail == nil || snapshotID == "" {
		return durableRuntimeCapabilitySnapshot{}, false, nil
	}
	for _, step := range detail.Steps {
		raw, exists := step.VerificationJSON["capability_snapshot"]
		if !exists || raw == nil {
			continue
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return durableRuntimeCapabilitySnapshot{}, true, fmt.Errorf("encode embedded capability snapshot from step %s: %w", step.StepID, err)
		}
		var snapshot durableRuntimeCapabilitySnapshot
		if err := json.Unmarshal(encoded, &snapshot); err != nil {
			return durableRuntimeCapabilitySnapshot{}, true, fmt.Errorf("decode embedded capability snapshot from step %s: %w", step.StepID, err)
		}
		if strings.TrimSpace(snapshot.SnapshotID) != snapshotID {
			continue
		}
		return snapshot, true, nil
	}
	return durableRuntimeCapabilitySnapshot{}, false, nil
}

type durableRuntimeCapabilitySnapshot struct {
	Schema         string                                     `json:"schema"`
	SnapshotID     string                                     `json:"snapshot_id"`
	SnapshotKind   string                                     `json:"snapshot_kind"`
	Status         durableRuntimeCapabilitySnapshotStatus     `json:"status"`
	Surfaces       map[string]durableRuntimeCapabilitySurface `json:"surfaces"`
	PromptContract durableRuntimeCapabilityPromptContract     `json:"prompt_contract"`
}

type durableRuntimeCapabilitySnapshotStatus struct {
	Overall string `json:"overall"`
}

type durableRuntimeCapabilityPromptContract struct {
	ContractID             string                                 `json:"contract_id"`
	EnabledToolNames       []string                               `json:"enabled_tool_names"`
	DisabledToolNames      []durableRuntimeCapabilityDisabledTool `json:"disabled_tool_names"`
	InspectionOnlySurfaces []string                               `json:"inspection_only_surfaces"`
	BudgetSummary          durableRuntimeCapabilityBudgetSummary  `json:"budget_summary"`
	MustInclude            []string                               `json:"must_include"`
}

type durableRuntimeCapabilityDisabledTool struct {
	Name       string `json:"name"`
	ReasonCode string `json:"reason_code"`
}

type durableRuntimeCapabilityBudgetSummary struct {
	MaxToolIterations   int `json:"max_tool_iterations"`
	MaxShellTimeoutSec  int `json:"max_shell_timeout_sec"`
	MaxPromptDocChars   int `json:"max_prompt_doc_chars"`
	MaxPromptSpecChars  int `json:"max_prompt_spec_chars"`
	MaxSmokeCyclesAgent int `json:"max_smoke_cycles_per_agent"`
	MaxSmokeCyclesTask  int `json:"max_smoke_cycles_per_task"`
}

type durableRuntimeCapabilitySurface struct {
	SurfaceID       string                                   `json:"surface_id"`
	Status          string                                   `json:"status"`
	DisabledReasons []durableRuntimeCapabilityDisabledReason `json:"disabled_reasons"`
}

type durableRuntimeCapabilityDisabledReason struct {
	Code string `json:"code"`
}

type durableRuntimeCapabilitySurfaceState struct {
	SurfaceID   string
	Status      string
	ReasonCodes []string
}

func validateDurableRuntimeCapabilitySnapshotReadback(proof map[string]any, accepted *durablePromptCompilerSnapshot) error {
	path := durableRuntimeRawMapString(proof, "capability_snapshot_path")
	if accepted != nil {
		accepted.CapabilitySnapshotPath = path
		accepted.SnapshotReadbackState = "missing"
	}
	if strings.TrimSpace(path) == "" {
		return errors.New("daemon prompt compiler proof is missing capability_snapshot_path for durable snapshot readback")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if accepted != nil {
			accepted.SnapshotReadbackState = "error"
		}
		return fmt.Errorf("read capability snapshot %q: %w", path, err)
	}
	var snapshot durableRuntimeCapabilitySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		if accepted != nil {
			accepted.SnapshotReadbackState = "error"
		}
		return fmt.Errorf("decode capability snapshot %q: %w", path, err)
	}
	return validateDurableRuntimeCapabilitySnapshotValue(snapshot, proof, accepted)
}

func validateDurableRuntimeCapabilitySnapshotValue(snapshot durableRuntimeCapabilitySnapshot, proof map[string]any, accepted *durablePromptCompilerSnapshot) error {
	if got, want := strings.TrimSpace(snapshot.SnapshotID), durableRuntimeRawMapString(proof, "capability_snapshot_id"); got != want {
		return fmt.Errorf("capability snapshot readback id %q differs from proof id %q", got, want)
	}
	if got, want := strings.TrimSpace(snapshot.Schema), durableRuntimeRawMapString(proof, "snapshot_schema"); got != want {
		return fmt.Errorf("capability snapshot readback schema %q differs from proof schema %q", got, want)
	}
	if got, want := strings.TrimSpace(snapshot.SnapshotKind), durableRuntimeRawMapString(proof, "snapshot_kind"); got != want {
		return fmt.Errorf("capability snapshot readback kind %q differs from proof kind %q", got, want)
	}
	if got, want := strings.TrimSpace(snapshot.Status.Overall), durableRuntimeRawMapString(proof, "snapshot_status"); got != want {
		return fmt.Errorf("capability snapshot readback status %q differs from proof status %q", got, want)
	}
	if got, want := strings.TrimSpace(snapshot.PromptContract.ContractID), durableRuntimeRawMapString(proof, "prompt_contract"); got != want {
		return fmt.Errorf("capability snapshot readback prompt contract %q differs from proof prompt contract %q", got, want)
	}
	projection := renderDurableRuntimeCapabilityPromptProjection(snapshot)
	digest := durableRuntimeCapabilityProjectionDigest(projection)
	if accepted != nil {
		if strings.TrimSpace(accepted.SnapshotReadbackState) == "" || accepted.SnapshotReadbackState == "missing" {
			accepted.SnapshotReadbackState = "ok"
		}
		accepted.SnapshotReadbackDigest = digest
	}
	if got, want := digest, durableRuntimeRawMapString(proof, "projection_digest"); got != want {
		return fmt.Errorf("capability snapshot readback projection_digest=%q differs from proof projection_digest=%q", got, want)
	}
	return nil
}

func renderDurableRuntimeCapabilityPromptProjection(snapshot durableRuntimeCapabilitySnapshot) string {
	if strings.TrimSpace(snapshot.SnapshotID) == "" {
		return ""
	}

	contract := snapshot.PromptContract
	enabledTools, contradictions := durableRuntimeCapabilityProjectionEnabledTools(contract)
	disabledTools := durableRuntimeCapabilityProjectionDisabledTools(contract)
	inspectionOnly := durableRuntimeUniqueTrimmedStrings(contract.InspectionOnlySurfaces)
	sort.Strings(inspectionOnly)
	surfaces := durableRuntimeCapabilityProjectionSurfaceStates(snapshot)

	var b strings.Builder
	b.WriteString("## Active Capability Snapshot\n")
	b.WriteString("- projection_source: agent.runtime_capability_snapshot\n")
	b.WriteString("- projection_contract: active_capability_snapshot_projection.v1\n")
	b.WriteString(fmt.Sprintf("- snapshot_id: %s\n", strings.TrimSpace(snapshot.SnapshotID)))
	if strings.TrimSpace(snapshot.SnapshotKind) != "" {
		b.WriteString(fmt.Sprintf("- snapshot_kind: %s\n", strings.TrimSpace(snapshot.SnapshotKind)))
	}
	if strings.TrimSpace(snapshot.Schema) != "" {
		b.WriteString(fmt.Sprintf("- schema: %s\n", strings.TrimSpace(snapshot.Schema)))
	}
	if strings.TrimSpace(snapshot.Status.Overall) != "" {
		b.WriteString(fmt.Sprintf("- overall_status: %s\n", strings.TrimSpace(snapshot.Status.Overall)))
	}
	if strings.TrimSpace(contract.ContractID) != "" {
		b.WriteString(fmt.Sprintf("- prompt_contract: %s\n", strings.TrimSpace(contract.ContractID)))
	}
	b.WriteString(fmt.Sprintf("- enabled_tools: %s\n", durableRuntimeCapabilityProjectionList(enabledTools)))

	if len(disabledTools) > 0 {
		b.WriteString("- disabled_tools:\n")
		for _, item := range durableRuntimeCapabilityProjectionLimitDisabled(disabledTools, 40) {
			b.WriteString(fmt.Sprintf("  - %s: disabled (%s)\n", item.Name, item.ReasonCode))
		}
		if len(disabledTools) > 40 {
			b.WriteString(fmt.Sprintf("  - ... %d more disabled tools omitted\n", len(disabledTools)-40))
		}
	}
	if len(inspectionOnly) > 0 {
		b.WriteString(fmt.Sprintf("- inspection_only_surfaces: %s\n", durableRuntimeCapabilityProjectionList(inspectionOnly)))
	}
	if len(surfaces) > 0 {
		b.WriteString("- surface_states:\n")
		for _, surface := range durableRuntimeCapabilityProjectionLimitSurfaces(surfaces, 40) {
			reasons := durableRuntimeCapabilityProjectionList(surface.ReasonCodes)
			if reasons == "(none)" {
				b.WriteString(fmt.Sprintf("  - %s: %s\n", surface.SurfaceID, surface.Status))
			} else {
				b.WriteString(fmt.Sprintf("  - %s: %s (%s)\n", surface.SurfaceID, surface.Status, reasons))
			}
		}
		if len(surfaces) > 40 {
			b.WriteString(fmt.Sprintf("  - ... %d more surface states omitted\n", len(surfaces)-40))
		}
	}
	b.WriteString("- budget_ceilings:\n")
	b.WriteString(fmt.Sprintf("  - max_tool_iterations: %d\n", contract.BudgetSummary.MaxToolIterations))
	b.WriteString(fmt.Sprintf("  - max_shell_timeout_sec: %d\n", contract.BudgetSummary.MaxShellTimeoutSec))
	b.WriteString(fmt.Sprintf("  - max_prompt_doc_chars: %d\n", contract.BudgetSummary.MaxPromptDocChars))
	b.WriteString(fmt.Sprintf("  - max_prompt_spec_chars: %d\n", contract.BudgetSummary.MaxPromptSpecChars))
	b.WriteString(fmt.Sprintf("  - max_smoke_cycles_per_agent: %d\n", contract.BudgetSummary.MaxSmokeCyclesAgent))
	b.WriteString(fmt.Sprintf("  - max_smoke_cycles_per_task: %d\n", contract.BudgetSummary.MaxSmokeCyclesTask))
	if len(contract.MustInclude) > 0 {
		b.WriteString("- hard_rules:\n")
		for _, rule := range durableRuntimeCapabilityProjectionLimitStrings(durableRuntimeCapabilityProjectionTrimmedStrings(contract.MustInclude), 40) {
			b.WriteString(fmt.Sprintf("  - %s\n", rule))
		}
	}
	if len(contradictions) > 0 {
		b.WriteString("- projection_warnings:\n")
		for _, warning := range durableRuntimeCapabilityProjectionLimitStrings(contradictions, 40) {
			b.WriteString(fmt.Sprintf("  - contract_violation: %s; treated as disabled\n", warning))
		}
	}

	return durableRuntimeInjectCapabilityProjectionDigest(strings.TrimSpace(b.String()))
}

func durableRuntimeInjectCapabilityProjectionDigest(projection string) string {
	projection = strings.TrimSpace(projection)
	if projection == "" {
		return ""
	}
	digest := durableRuntimeCapabilityProjectionDigest(projection)
	lines := strings.Split(projection, "\n")
	insertAt := 1
	for insertAt < len(lines) {
		trimmed := strings.TrimSpace(lines[insertAt])
		if !strings.HasPrefix(trimmed, "- projection_") {
			break
		}
		insertAt++
	}
	out := make([]string, 0, len(lines)+1)
	out = append(out, lines[:insertAt]...)
	out = append(out, "- projection_digest: "+digest)
	out = append(out, lines[insertAt:]...)
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func durableRuntimeCapabilityProjectionDigest(section string) string {
	return "sha256:" + durableRuntimeSHA256Hex(durableRuntimeStripCapabilityProjectionDigest(section))
}

func durableRuntimeStripCapabilityProjectionDigest(section string) string {
	lines := strings.Split(strings.TrimSpace(section), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if strings.HasPrefix(trimmed, "projection_digest:") {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func durableRuntimeSHA256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func durableRuntimeCapabilityProjectionEnabledTools(contract durableRuntimeCapabilityPromptContract) ([]string, []string) {
	disabled := durableRuntimeCapabilityProjectionDisabledTools(contract)
	enabled := durableRuntimeUniqueTrimmedStrings(contract.EnabledToolNames)
	filtered := make([]string, 0, len(enabled))
	contradictions := []string{}
	for _, name := range enabled {
		if disabledBy, ok := durableRuntimeCapabilityProjectionDisabledMatch(name, disabled); ok {
			contradictions = append(contradictions, fmt.Sprintf("enabled_tool %s conflicts with disabled %s", name, disabledBy))
			continue
		}
		filtered = append(filtered, name)
	}
	sort.Strings(filtered)
	sort.Strings(contradictions)
	return filtered, contradictions
}

func durableRuntimeCapabilityProjectionDisabledTools(contract durableRuntimeCapabilityPromptContract) []durableRuntimeCapabilityDisabledTool {
	items := make([]durableRuntimeCapabilityDisabledTool, 0, len(contract.DisabledToolNames))
	for _, item := range contract.DisabledToolNames {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		reason := strings.TrimSpace(item.ReasonCode)
		if reason == "" {
			reason = "unspecified"
		}
		items = append(items, durableRuntimeCapabilityDisabledTool{Name: name, ReasonCode: reason})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].ReasonCode < items[j].ReasonCode
		}
		return items[i].Name < items[j].Name
	})
	return items
}

func durableRuntimeCapabilityProjectionDisabledMatch(name string, disabled []durableRuntimeCapabilityDisabledTool) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	for _, item := range disabled {
		disabledName := strings.TrimSpace(item.Name)
		if disabledName == name {
			return disabledName, true
		}
		if strings.HasSuffix(disabledName, ".*") {
			prefix := strings.TrimSuffix(disabledName, ".*")
			if strings.HasPrefix(name, prefix+".") || strings.HasPrefix(name, prefix+"_") {
				return disabledName, true
			}
		}
		if strings.HasSuffix(disabledName, "*") {
			prefix := strings.TrimSuffix(disabledName, "*")
			if prefix != "" && strings.HasPrefix(name, prefix) {
				return disabledName, true
			}
		}
	}
	return "", false
}

func durableRuntimeCapabilityProjectionSurfaceStates(snapshot durableRuntimeCapabilitySnapshot) []durableRuntimeCapabilitySurfaceState {
	states := make([]durableRuntimeCapabilitySurfaceState, 0, len(snapshot.Surfaces))
	for id, surface := range snapshot.Surfaces {
		status := strings.TrimSpace(surface.Status)
		if status == "" || strings.EqualFold(status, "enabled") {
			continue
		}
		surfaceID := firstNonEmpty(strings.TrimSpace(surface.SurfaceID), strings.TrimSpace(id))
		if surfaceID == "" {
			continue
		}
		reasons := make([]string, 0, len(surface.DisabledReasons))
		for _, reason := range surface.DisabledReasons {
			if code := strings.TrimSpace(reason.Code); code != "" {
				reasons = append(reasons, code)
			}
		}
		reasons = durableRuntimeUniqueTrimmedStrings(reasons)
		sort.Strings(reasons)
		states = append(states, durableRuntimeCapabilitySurfaceState{
			SurfaceID:   surfaceID,
			Status:      status,
			ReasonCodes: reasons,
		})
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Status == states[j].Status {
			return states[i].SurfaceID < states[j].SurfaceID
		}
		return states[i].Status < states[j].Status
	})
	return states
}

func durableRuntimeCapabilityProjectionList(items []string) string {
	items = durableRuntimeUniqueTrimmedStrings(items)
	sort.Strings(items)
	if len(items) == 0 {
		return "(none)"
	}
	limited := durableRuntimeCapabilityProjectionLimitStrings(items, 40)
	value := strings.Join(limited, ", ")
	if len(items) > len(limited) {
		value += fmt.Sprintf(", ... %d more", len(items)-len(limited))
	}
	return value
}

func durableRuntimeUniqueTrimmedStrings(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func durableRuntimeCapabilityProjectionTrimmedStrings(items []string) []string {
	return durableRuntimeUniqueTrimmedStrings(items)
}

func durableRuntimeCapabilityProjectionLimitStrings(items []string, limit int) []string {
	if limit <= 0 || len(items) <= limit {
		return append([]string(nil), items...)
	}
	return append([]string(nil), items[:limit]...)
}

func durableRuntimeCapabilityProjectionLimitDisabled(items []durableRuntimeCapabilityDisabledTool, limit int) []durableRuntimeCapabilityDisabledTool {
	if limit <= 0 || len(items) <= limit {
		return append([]durableRuntimeCapabilityDisabledTool(nil), items...)
	}
	return append([]durableRuntimeCapabilityDisabledTool(nil), items[:limit]...)
}

func durableRuntimeCapabilityProjectionLimitSurfaces(items []durableRuntimeCapabilitySurfaceState, limit int) []durableRuntimeCapabilitySurfaceState {
	if limit <= 0 || len(items) <= limit {
		return append([]durableRuntimeCapabilitySurfaceState(nil), items...)
	}
	return append([]durableRuntimeCapabilitySurfaceState(nil), items[:limit]...)
}

func durableRuntimeCapabilitySnapshotRefFromStep(step sqlite.ExecutionStepRecord) (string, string) {
	refs := make([]string, 0, 3)
	for _, ref := range step.Evidence {
		ref = strings.TrimSpace(ref)
		if strings.HasPrefix(ref, "capability_snapshot:") {
			refs = append(refs, ref)
		}
	}
	if ref := durableRuntimeRawMapString(step.VerificationJSON, "capability_snapshot_ref"); ref != "" {
		refs = append(refs, ref)
	}
	if snapshotID := durableRuntimeRawMapString(step.VerificationJSON, "capability_snapshot_id"); snapshotID != "" {
		if !durableRuntimeCanonicalCapabilitySnapshotID(snapshotID) {
			return "", fmt.Sprintf("execution step has non-canonical capability snapshot id %q", snapshotID)
		}
		refs = append(refs, "capability_snapshot:"+snapshotID)
	}
	if proof, ok := step.VerificationJSON["prompt_capability_evidence"].(map[string]any); ok {
		if ref := durableRuntimeRawMapString(proof, "capability_snapshot_ref"); ref != "" {
			refs = append(refs, ref)
		}
	}
	canonicalRef := ""
	for _, ref := range refs {
		snapshotID := strings.TrimPrefix(ref, "capability_snapshot:")
		if snapshotID == ref || !durableRuntimeCanonicalCapabilitySnapshotID(snapshotID) {
			return "", fmt.Sprintf("execution step has non-canonical capability snapshot ref %q", ref)
		}
		if canonicalRef == "" {
			canonicalRef = ref
			continue
		}
		if canonicalRef != ref {
			return "", fmt.Sprintf("execution step has multiple capability snapshot refs %q and %q", canonicalRef, ref)
		}
	}
	return canonicalRef, ""
}

func durableRuntimeEvidenceRefsContain(evidence []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, ref := range evidence {
		if strings.TrimSpace(ref) == want {
			return true
		}
	}
	return false
}

func durableRuntimePromptMarkerEvidenceRef(evidence []string) string {
	for _, ref := range evidence {
		normalized := strings.ToLower(strings.TrimSpace(ref))
		if normalized == "" {
			continue
		}
		for _, marker := range []string{
			"prompt_compiler_status:",
			"c2_1_convergence:",
			"deployment_evidence:",
			"projection_digest:",
			"projection_source:",
			"projection_contract:",
			"prompt_contract:",
			"prompt_capability_evidence:",
			"daemon_prompt_compiler_proof:",
			"daemon_prompt_capability_evidence:",
			"capability_snapshot_ref:",
			durablePromptCapabilityEvidenceContract,
			"legacy_non_converged",
			"excluded_until_migrated",
			"not_accepted_for_daemon_prompt_compiler_convergence",
		} {
			if strings.Contains(normalized, marker) {
				return strings.TrimSpace(ref)
			}
		}
	}
	return ""
}

func durableRuntimeLegacyPromptVerificationIssue(values map[string]any) string {
	if issue := durableRuntimeLegacyPromptValueIssue("verification", values); issue != "" {
		return issue
	}
	return ""
}

func durableRuntimeLegacyPromptValueIssue(path string, value any) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			nextPath := path + "." + strings.TrimSpace(key)
			switch key {
			case "prompt_compiler_status", "c2_1_convergence", "deployment_evidence":
				text, ok := nested.(string)
				if !ok {
					return nextPath + " is not a canonical string"
				}
				normalized := strings.ToLower(strings.TrimSpace(text))
				if strings.Contains(normalized, "legacy") ||
					strings.Contains(normalized, "non_converged") ||
					normalized == "excluded_until_migrated" ||
					normalized == "not_accepted_for_daemon_prompt_compiler_convergence" ||
					normalized == "absent" {
					return nextPath + " contains legacy/non-converged prompt evidence"
				}
			}
			if issue := durableRuntimeLegacyPromptValueIssue(nextPath, nested); issue != "" {
				return issue
			}
		}
	case []any:
		for idx, nested := range typed {
			if issue := durableRuntimeLegacyPromptValueIssue(fmt.Sprintf("%s[%d]", path, idx), nested); issue != "" {
				return issue
			}
		}
	}
	return ""
}

func durableRuntimeCanonicalSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, ch := range strings.TrimPrefix(value, "sha256:") {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func durableRuntimeCanonicalCapabilitySnapshotID(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || !strings.HasPrefix(value, "cap_") {
		return false
	}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '_' || ch == '-':
		default:
			return false
		}
	}
	return true
}

func durableRuntimeMapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func durableRuntimeRawMapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch value := values[key].(type) {
	case string:
		return value
	default:
		return ""
	}
}

func durableRuntimeNestedMapString(values map[string]any, key, nestedKey string) string {
	if values == nil {
		return ""
	}
	nested, ok := values[key].(map[string]any)
	if !ok {
		return ""
	}
	return durableRuntimeMapString(nested, nestedKey)
}

func durableRuntimeProgressToken(values map[string]any) string {
	keys := []string{
		"checkpoint_id",
		"checkpoint",
		"progress_id",
		"progress",
		"cursor_id",
		"cursor",
		"step_id",
		"next_step_id",
	}
	for _, key := range keys {
		if token := durableRuntimeJSONToken(values[key]); token != "" {
			return key + "=" + token
		}
	}
	return ""
}

func durableRuntimeJSONToken(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(raw))
	}
}

func durableRuntimeStepMoreRecent(candidate, current sqlite.ExecutionStepRecord) bool {
	if candidate.UpdatedAt != current.UpdatedAt {
		return candidate.UpdatedAt > current.UpdatedAt
	}
	if candidate.CreatedAt != current.CreatedAt {
		return candidate.CreatedAt > current.CreatedAt
	}
	return candidate.StepID > current.StepID
}

func collectServiceMetricsSummary(metricsPath string) serviceMetricsSummary {
	summary := serviceMetricsSummary{
		Status:     "unknown",
		SourcePath: strings.TrimSpace(metricsPath),
		Health:     evaluateRuntimeMetricsHealth(nil),
	}
	if summary.SourcePath == "" {
		summary.Status = "missing"
		summary.Error = "metrics path is empty"
		return summary
	}

	snapshots, totalValid, parseErrors, err := readRuntimeMetricsSnapshots(summary.SourcePath, 5)
	summary.SnapshotsLoaded = len(snapshots)
	summary.SnapshotsTotalValid = totalValid
	summary.ParseErrors = parseErrors
	if err != nil {
		if strings.Contains(err.Error(), "metrics file not found") {
			summary.Status = "missing"
			summary.Error = err.Error()
			return summary
		}
		summary.Status = "error"
		summary.Error = err.Error()
		return summary
	}

	var latest *runtimeMetricsSnapshot
	if len(snapshots) > 0 {
		last := snapshots[len(snapshots)-1]
		latest = &last
		summary.LatestTimestamp = last.Timestamp
	}
	summary.Health = evaluateRuntimeMetricsHealth(latest)

	if parseErrors == 0 && summary.Health.Verdict == "unknown" {
		// A valid but still-idle metrics stream should not degrade the whole
		// service before any real runtime samples have been produced.
		summary.Status = "ok"
		return summary
	}
	if parseErrors > 0 || summary.Health.Verdict == "degraded" {
		summary.Status = "degraded"
		return summary
	}
	if summary.Health.Verdict == "healthy" {
		summary.Status = "ok"
		return summary
	}
	summary.Status = "unknown"
	return summary
}

func fetchServiceHealth(ctx context.Context, healthURL string, token string) (serviceHealthPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return serviceHealthPayload{}, fmt.Errorf("build health request: %w", err)
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return serviceHealthPayload{}, fmt.Errorf("request health endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return serviceHealthPayload{}, fmt.Errorf("health endpoint returned status %d", resp.StatusCode)
	}

	var payload serviceHealthPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return serviceHealthPayload{}, fmt.Errorf("decode health payload: %w", err)
	}
	if strings.TrimSpace(payload.Status) == "" {
		return serviceHealthPayload{}, fmt.Errorf("decode health payload: missing status")
	}
	return payload, nil
}

func isLoopbackHealthURL(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func diffConfigSnapshots(local configSnapshot, live configSnapshot) []map[string]string {
	diffs := make([]map[string]string, 0, 5)
	appendDiff := func(field, localValue, liveValue string) {
		if pathsEqual(localValue, liveValue) {
			return
		}
		diffs = append(diffs, map[string]string{
			"field":   field,
			"local":   localValue,
			"service": liveValue,
		})
	}

	appendDiff("db_path", local.DBPath, live.DBPath)
	appendDiff("workspace_root", local.WorkspaceRoot, live.WorkspaceRoot)
	appendDiff("metrics_path", local.MetricsPath, live.MetricsPath)
	appendDiff("executor_python", local.ExecutorPython, live.ExecutorPython)
	appendDiff("executor_bridge_script", local.ExecutorBridgeScript, live.ExecutorBridgeScript)
	return diffs
}

func pathsEqual(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return left == right
	}

	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func shortRevision(rev string) string {
	rev = strings.TrimSpace(rev)
	if len(rev) > 12 {
		return rev[:12]
	}
	return rev
}
