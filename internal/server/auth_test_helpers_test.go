package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func withTestAgentPrincipal(ctx context.Context, workspaceID, agentID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if principal, ok := authPrincipalFromContext(ctx); ok &&
		strings.TrimSpace(principal.WorkspaceID) != "" &&
		strings.TrimSpace(principal.PrincipalID) != "" {
		return ctx
	}
	return context.WithValue(ctx, authPrincipalContextKey{}, AuthPrincipal{
		WorkspaceID:   strings.TrimSpace(workspaceID),
		PrincipalType: "agent",
		PrincipalID:   strings.TrimSpace(agentID),
	})
}

func callAgentMessageSendRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentMessageSendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal send params for auth wrapper: %v", err)
	}
	return h.agentMessageSend(withTestAgentPrincipal(ctx, p.WorkspaceID, p.FromAgentID), raw)
}

func callAgentMessagePollRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentMessagePollParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal poll params for auth wrapper: %v", err)
	}
	return h.agentMessagePoll(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentMessageAckRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentMessageAckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal ack params for auth wrapper: %v", err)
	}
	return h.agentMessageAck(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentRequestRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentRequestParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal request params for auth wrapper: %v", err)
	}
	return h.agentRequest(withTestAgentPrincipal(ctx, p.WorkspaceID, p.FromAgentID), raw)
}

func callAgentRequestListRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentRequestListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal request list params for auth wrapper: %v", err)
	}
	return h.agentRequestList(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentRequestOpenListRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentRequestOpenListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal request open-list params for auth wrapper: %v", err)
	}
	return h.agentRequestOpenList(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentRespondRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentRespondParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal respond params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		req, err := h.store.GetAgentRequestResult(context.Background(), strings.TrimSpace(p.RequestID))
		if err == nil {
			ctx = withTestAgentPrincipal(ctx, req.WorkspaceID, req.ToAgentID)
		}
	}
	return h.agentRespond(ctx, raw)
}

func callAgentRequestResultRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentRequestResultParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal request result params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		req, err := h.store.GetAgentRequestResult(context.Background(), strings.TrimSpace(p.RequestID))
		if err == nil {
			ctx = withTestAgentPrincipal(ctx, req.WorkspaceID, req.FromAgentID)
		}
	}
	return h.agentRequestResult(ctx, raw)
}

func callAgentSessionStartRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentSessionEventParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal session start params for auth wrapper: %v", err)
	}
	return h.agentSessionStart(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentSessionBlockedRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentSessionEventParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal session blocked params for auth wrapper: %v", err)
	}
	return h.agentSessionBlocked(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callAgentSessionTakeoverRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p agentSessionTakeoverParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal session takeover params for auth wrapper: %v", err)
	}
	return h.agentSessionTakeover(withTestAgentPrincipal(ctx, p.WorkspaceID, p.TakeoverAgentID), raw)
}

func callWorkspaceMemoryInvalidationPollRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationPollParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation poll params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationPoll(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationAckRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationAckParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation ack params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationAck(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationFailRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationFailParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation fail params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationFail(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationRequeueRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationRequeueParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation requeue params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationRequeue(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationListRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation list params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationList(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationGetRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation get params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationGet(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryInvalidationCursorGetRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryInvalidationCursorGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory invalidation cursor params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryInvalidationCursorGet(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryResidencyReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p sqlite.MemoryResidencyReportInput
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory residency report params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryResidencyReport(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryResidencyListRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryResidencyListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory residency list params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryResidencyList(ctx, raw)
}

func callWorkspaceMemoryResidencyGetRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryResidencyGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory residency get params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryResidencyGet(ctx, raw)
}

func callWorkspaceMemoryMetricsReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p sqlite.MemoryMetricsReportInput
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory metrics report params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryMetricsReport(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryMetricsListRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryMetricsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory metrics list params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryMetricsList(ctx, raw)
}

func callWorkspaceMemoryMetricsGetRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryMetricsGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory metrics get params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryMetricsGet(ctx, raw)
}

func callWorkspaceMemoryCoherenceReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryCoherenceReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory coherence report params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryCoherenceReport(ctx, raw)
}

func callWorkspaceMemoryCoherenceScopeRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryCoherenceScopeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory coherence scope params for auth wrapper: %v", err)
	}
	return h.workspaceMemoryCoherenceScope(withTestAgentPrincipal(ctx, p.WorkspaceID, p.AgentID), raw)
}

func callWorkspaceMemoryCoherenceSnapshotRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceMemoryCoherenceReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal memory coherence snapshot params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceMemoryCoherenceSnapshot(ctx, raw)
}

func callWorkspaceRSPForecastReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPForecastReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp forecast report params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPForecastReport(ctx, raw)
}

func callWorkspaceRSPForecastSnapshotRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPForecastReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp forecast snapshot params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPForecastSnapshot(ctx, raw)
}

func callWorkspaceRSPBeliefReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPBeliefReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp belief report params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPBeliefReport(ctx, raw)
}

func callWorkspaceRSPBeliefClaimRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPBeliefClaimParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp belief claim params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPBeliefClaim(ctx, raw)
}

func callWorkspaceRSPBeliefSnapshotRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPBeliefReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp belief snapshot params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPBeliefSnapshot(ctx, raw)
}

func callWorkspaceRSPStateReportRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPStateReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp state report params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPStateReport(ctx, raw)
}

func callWorkspaceRSPStateSnapshotRaw(t *testing.T, h *Handler, ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	t.Helper()
	var p workspaceRSPStateReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal rsp state snapshot params for auth wrapper: %v", err)
	}
	if _, ok := authPrincipalFromContext(ctx); !ok {
		ctx = testAuthContext(strings.TrimSpace(p.WorkspaceID), "human", "developer")
	}
	return h.workspaceRSPStateSnapshot(ctx, raw)
}
