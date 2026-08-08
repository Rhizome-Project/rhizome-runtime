package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type WorkspaceEpochTickInput struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) workspaceControlEpochTick(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	var input WorkspaceEpochTickInput
	if err := json.Unmarshal(params, &input); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}

	// 1. Advance the explicit Control Epoch
	_, err := h.store.IncrementEpoch(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to increment epoch: %v", err)}
	}

	// 2. Compute Read-Side Metrics (using Corridor Fit Approximation)
	fitReport, err := h.store.BuildCorridorFitReport(ctx, sqlite.CorridorFitFilter{
		WorkspaceID: workspaceID,
		Limit:       1, // Use the dominant cluster for system-wide workspace constraints
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to build fit report: %v", err)}
	}

	// Extract the most relevant metric gaps. If we have at least one cluster, use it.
	var gaps []sqlite.CorridorFitMetricGap
	if len(fitReport.Clusters) > 0 {
		gaps = fitReport.Clusters[0].MetricGapBreakdown
	}

	// 3. Evaluate control proposal without mutating live policy.
	preview, err := h.store.EvaluateClusterPolicy(ctx, workspaceID, gaps)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to evaluate policy engine: %v", err)}
	}

	// 4. Cleanup old resolved tensions (TTL: 24h)
	_, _ = h.store.ArchiveResolvedTensions(ctx, workspaceID, 24)

	// 5. Sweep for new coalition formations missing from active tensions
	err = h.store.CoalitionFormationSweep(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("failed to execute coalition sweep: %v", err)}
	}

	return map[string]any{
		"status":                 "ok",
		"epoch_advanced":         true,
		"control_policy_preview": preview,
	}, nil
}
