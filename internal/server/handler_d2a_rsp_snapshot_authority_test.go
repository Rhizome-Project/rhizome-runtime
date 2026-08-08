package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPForecastReportRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPForecastAuthorityScenario(t, ctx, store, "report-mismatch")

	result, rpcErr := callWorkspaceRSPForecastReportRaw(t, h, testAuthContext("ws-other-rsp-forecast", "human", "developer"), mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPForecastSnapshotRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPForecastAuthorityScenario(t, ctx, store, "missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPForecastSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.rsp.forecast.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.forecast_snapshot")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority forecast snapshot reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceRSPForecastSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPForecastAuthorityScenario(t, ctx, store, "stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4101")

	result, rpcErr := callWorkspaceRSPForecastSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.rsp.forecast.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.forecast_snapshot")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale forecast snapshot reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceRSPBeliefReportRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "report-mismatch")

	result, rpcErr := callWorkspaceRSPBeliefReportRaw(t, h, testAuthContext("ws-other-rsp-belief", "human", "developer"), mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPBeliefClaimRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "claim-mismatch")

	result, rpcErr := callWorkspaceRSPBeliefClaimRaw(t, h, testAuthContext("ws-other-rsp-belief", "human", "developer"), mustJSONRaw(workspaceRSPBeliefClaimParams{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     scenario.claimID,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPBeliefSnapshotRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPBeliefSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.rsp.belief.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.belief_snapshot")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority belief snapshot reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceRSPBeliefSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4102")

	result, rpcErr := callWorkspaceRSPBeliefSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.rsp.belief.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.belief_snapshot")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale belief snapshot reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func TestWorkspaceRSPStateReportRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPStateAuthorityScenario(t, ctx, store, "report-mismatch")

	result, rpcErr := callWorkspaceRSPStateReportRaw(t, h, testAuthContext("ws-other-rsp-state", "human", "developer"), mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPStateSnapshotRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPStateAuthorityScenario(t, ctx, store, "missing-authority")
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectMissing, "workspace.rsp.state.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.state_snapshot")
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority state snapshot reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceRSPStateSnapshotRejectsStaleWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPStateAuthorityScenario(t, ctx, store, "stale-authority")
	current := claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-4103")

	result, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, testAuthContext(scenario.workspaceID, "human", "developer"), mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale authority reject, got %+v", result)
	}
	assertTaskAuthorityRejectDetails(t, rpcErr, sqlite.AuthorityRejectStale, "workspace.rsp.state.snapshot")
	assertNoServerRuntimeEventsOfType(t, ctx, store, scenario.workspaceID, "rsp.state_snapshot")
	assertServerTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected stale state snapshot reject to advance workspace updated_at due to reject journaling, still %q", afterUpdatedAt)
	}
}

func seedHandlerRSPForecastAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) locusSidecarServerScenario {
	t.Helper()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "authority-"+suffix)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.forecast.shadow",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable forecast snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable forecast shadow capability: %v", err)
	}
	return scenario
}

func seedHandlerRSPStateAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) locusSidecarServerScenario {
	t.Helper()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "state-authority-"+suffix)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.state.shadow",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable state snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable state shadow capability: %v", err)
	}
	return scenario
}

type handlerRSPBeliefAuthorityScenario struct {
	workspaceID string
	taskID      string
	claimID     string
}

func seedHandlerRSPBeliefAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) handlerRSPBeliefAuthorityScenario {
	t.Helper()

	scenario := handlerRSPBeliefAuthorityScenario{
		workspaceID: "ws-handler-rsp-belief-authority-" + suffix,
		taskID:      "task-handler-rsp-belief-authority-" + suffix,
		claimID:     "claim-handler-rsp-belief-authority-" + suffix,
	}

	seedHandlerAgentWorkWorkspace(t, ctx, store, scenario.workspaceID, []string{"agent-a", "agent-b"})
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	createHandlerAgentWorkTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "high")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      "doc-handler-rsp-belief-authority-" + suffix,
		Title:       "RSP Belief Authority Doc",
		Content:     "Operators verified the rollout guardrail.",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     scenario.claimID,
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "Belief authority target",
		Body:        "This claim exists to exercise belief report authority enforcement.",
		Summary:     "Belief authority target.",
		Confidence:  0.92,
		SourceKind:  "workspace_doc",
		SourceID:    "doc-handler-rsp-belief-authority-" + suffix,
		TaskID:      scenario.taskID,
	}); err != nil {
		t.Fatalf("record belief authority claim: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  "rsp.belief.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief snapshot authority tests",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}
	return scenario
}

func TestWorkspaceRSPForecastSnapshotRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPForecastAuthorityScenario(t, ctx, store, "snapshot-mismatch")

	result, rpcErr := callWorkspaceRSPForecastSnapshotRaw(t, h, testAuthContext("ws-other-rsp-forecast", "human", "developer"), mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPBeliefSnapshotRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "snapshot-mismatch")

	result, rpcErr := callWorkspaceRSPBeliefSnapshotRaw(t, h, testAuthContext("ws-other-rsp-belief", "human", "developer"), mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPStateSnapshotRejectsWorkspacePrincipalMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPStateAuthorityScenario(t, ctx, store, "snapshot-mismatch")

	result, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, testAuthContext("ws-other-rsp-state", "human", "developer"), mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "workspace isolation violation" {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPBeliefClaimRequiresClaimIDStillBeforeAuth(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	result, rpcErr := h.workspaceRSPBeliefClaim(context.Background(), mustJSONRaw(workspaceRSPBeliefClaimParams{
		WorkspaceID: "ws-rsp-belief-missing-claim-auth-order",
	}))
	if rpcErr == nil {
		t.Fatal("expected missing claim_id error")
	}
	if result != nil {
		t.Fatalf("expected no result on invalid params, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPForecastReportRequiresWorkspaceIDStillBeforeAuth(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	result, rpcErr := h.workspaceRSPForecastReport(context.Background(), mustJSONRaw(workspaceRSPForecastReportParams{
		AgentID: "agent-a",
	}))
	if rpcErr == nil {
		t.Fatal("expected missing workspace_id invalid params error")
	}
	if result != nil {
		t.Fatalf("expected no result on invalid params, got %+v", result)
	}
	if rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("unexpected rpc error %+v", rpcErr)
	}
}

func TestWorkspaceRSPBeliefSnapshotRejectsWorkspacePrincipalMismatchBeforeStore(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPBeliefAuthorityScenario(t, ctx, store, "snapshot-mismatch-order")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPBeliefSnapshotRaw(t, h, testAuthContext("ws-other-rsp-belief", "human", "developer"), mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected principal mismatch to stop before store mutation, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceRSPForecastSnapshotRejectsWorkspacePrincipalMismatchBeforeStore(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPForecastAuthorityScenario(t, ctx, store, "snapshot-mismatch-order")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPForecastSnapshotRaw(t, h, testAuthContext("ws-other-rsp-forecast", "human", "developer"), mustJSONRaw(workspaceRSPForecastReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected principal mismatch to stop before store mutation, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceRSPStateSnapshotRejectsWorkspacePrincipalMismatchBeforeStore(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerRSPStateAuthorityScenario(t, ctx, store, "snapshot-mismatch-order")
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)

	result, rpcErr := callWorkspaceRSPStateSnapshotRaw(t, h, testAuthContext("ws-other-rsp-state", "human", "developer"), mustJSONRaw(workspaceRSPStateReportParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr == nil {
		t.Fatal("expected workspace isolation violation")
	}
	if result != nil {
		t.Fatalf("expected no result on permission denied, got %+v", result)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected principal mismatch to stop before store mutation, before=%q after=%q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func mustMarshalRSPAuthorityInput(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal rsp authority input: %v", err)
	}
	return raw
}
