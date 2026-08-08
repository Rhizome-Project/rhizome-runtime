package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceRSPBeliefReportClaimAndSnapshot(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-belief"
		taskID      = "task-handler-rsp-belief"
		docKey      = "doc-handler-rsp-belief"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "RSP Belief Doc",
		Content:     "Operators verified the deployment guardrail.",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-rsp-validator",
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "Verifier trace",
		Body:        "Verifier confirms the deployment guardrail.",
		Summary:     "Verifier confirms the guardrail.",
		Confidence:  0.95,
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		TaskID:      taskID,
	}); err != nil {
		t.Fatalf("record validator claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-rsp-fact",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Deployment guardrail",
		Body:        "Deploy only inside the approved maintenance window.",
		Summary:     "Deployment must stay inside the maintenance window.",
		Confidence:  0.87,
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		TaskID:      taskID,
		Evidence:    []string{"validated_by:claim-handler-rsp-validator"},
	}); err != nil {
		t.Fatalf("record fact claim: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.belief.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief calibration report for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}

	reportAny, rpcErr := callWorkspaceRSPBeliefReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPBeliefReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPBeliefReport)
	if report.Count == 0 || len(report.Items) == 0 {
		t.Fatalf("unexpected rsp belief report %+v", report)
	}
	if report.TimeAuthority.WorkspaceID != workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief rpc to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp belief rpc generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	if !report.CapabilityFlags.BeliefLive {
		t.Fatalf("expected rsp belief report to expose belief_live capability, got %+v", report.CapabilityFlags)
	}
	var factClaim sqlite.RSPBeliefClaimReport
	for _, item := range report.Items {
		if item.ClaimID == "claim-handler-rsp-fact" {
			factClaim = item
			break
		}
	}
	if factClaim.ClaimID == "" || len(factClaim.EvidenceRefs) == 0 {
		t.Fatalf("expected rsp belief report to surface claim evidence refs, got %+v", report.Items)
	}
	if factClaim.EvidenceUnitCount < 2 || factClaim.SourceDiversity <= 0 || factClaim.IndependenceDiscount <= 0 {
		t.Fatalf("expected rsp belief report to surface additive independence metrics, got %+v", factClaim)
	}

	itemAny, rpcErr := callWorkspaceRSPBeliefClaimRaw(t, h, ctx, mustJSONRaw(workspaceRSPBeliefClaimParams{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-handler-rsp-fact",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPBeliefClaim rpc error: %+v", rpcErr)
	}
	item := itemAny.(sqlite.RSPBeliefClaimReport)
	if item.ClaimID != "claim-handler-rsp-fact" || item.SignalType != "BELIEF_UPDATE" {
		t.Fatalf("unexpected rsp belief claim payload %+v", item)
	}
	if item.TimeAuthority.WorkspaceID != workspaceID || item.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief claim rpc to expose workspace time authority, got %+v", item.TimeAuthority)
	}
	if item.SourceDiversity <= 0 || item.IndependenceDiscount <= 0 || item.IndependentGroups == 0 {
		t.Fatalf("expected rsp belief claim rpc to expose additive independence metrics, got %+v", item)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.belief.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief snapshot for handler test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	snapshotAny, rpcErr := callWorkspaceRSPBeliefSnapshotRaw(t, h, ctx, mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPBeliefSnapshot rpc error: %+v", rpcErr)
	}
	snapshotPayload := snapshotAny.(map[string]any)
	snapshotReport := snapshotPayload["report"].(sqlite.RSPBeliefReport)
	if snapshotReport.TimeAuthority.WorkspaceID != workspaceID || snapshotReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief snapshot rpc to expose workspace time authority, got %+v", snapshotReport.TimeAuthority)
	}
	if snapshotReport.GeneratedAt != snapshotReport.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp belief snapshot rpc generated_at %q to mirror authority reference_at %q", snapshotReport.GeneratedAt, snapshotReport.TimeAuthority.ReferenceAt)
	}
	if !snapshotReport.CapabilityFlags.BeliefLive {
		t.Fatalf("expected rsp belief snapshot to keep belief_live capability visible, got %+v", snapshotReport.CapabilityFlags)
	}
	event := snapshotPayload["event"].(sqlite.RuntimeEventRecord)
	if event.EventType != "rsp.belief_snapshot" || event.EntityType != "rsp_belief" {
		t.Fatalf("unexpected rsp belief snapshot event %+v", event)
	}
	expectMemoryInvalidationEvent(t, ch, "rsp.belief_snapshot")
}

func TestWorkspaceRSPBeliefClaimRequiresClaimID(t *testing.T) {
	t.Parallel()

	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceRSPBeliefClaim(context.Background(), mustJSONRaw(workspaceRSPBeliefClaimParams{
		WorkspaceID: "ws-rsp-belief-missing-claim",
	})); rpcErr == nil {
		t.Fatal("expected missing claim_id error")
	}
}

func TestWorkspaceRSPBeliefReportSurfacesAggregateDiagnostics(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-rsp-belief-diagnostics"
		taskID      = "task-handler-rsp-belief-diagnostics"
	)

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a"})
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")
	for _, doc := range []string{"doc-handler-rsp-belief-shared", "doc-handler-rsp-belief-disputed"} {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc,
			Title:       doc,
			Content:     "handler belief diagnostics fixture " + doc,
			UpdatedBy:   "tests",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc, err)
		}
	}

	for _, claim := range []sqlite.KnowledgeClaimInput{
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-handler-rsp-belief-validator-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Shared validator A",
			Body:        "Shared validator A confirms the same-source belief claim.",
			Summary:     "Shared validator A.",
			Confidence:  0.94,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-handler-rsp-belief-shared",
			TaskID:      taskID,
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-handler-rsp-belief-validator-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Shared validator B",
			Body:        "Shared validator B confirms the same-source belief claim.",
			Summary:     "Shared validator B.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-handler-rsp-belief-shared",
			TaskID:      taskID,
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-handler-rsp-belief-same-source",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Same-source target",
			Body:        "Multiple confirmations all resolve to the same source.",
			Summary:     "Same-source target.",
			Confidence:  0.84,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-handler-rsp-belief-shared",
			TaskID:      taskID,
			Evidence: []string{
				"validated_by:claim-handler-rsp-belief-validator-a",
				"supports:claim-handler-rsp-belief-validator-b",
			},
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-handler-rsp-belief-disputed",
			ClaimType:   "FACT",
			Status:      "DISPUTED",
			Subject:     "Disputed target",
			Body:        "This belief item remains disputed and verifier-stale.",
			Summary:     "Disputed target.",
			Confidence:  0.41,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-handler-rsp-belief-disputed",
			TaskID:      taskID,
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record diagnostics claim %s: %v", claim.ClaimID, err)
		}
	}

	reportAny, rpcErr := callWorkspaceRSPBeliefReportRaw(t, h, ctx, mustJSONRaw(workspaceRSPBeliefReportParams{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceRSPBeliefReport rpc error: %+v", rpcErr)
	}
	report := reportAny.(sqlite.RSPBeliefReport)
	expected, err := store.BuildRSPBeliefReport(ctx, sqlite.RSPBeliefReportFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build expected rsp belief report: %v", err)
	}
	if report.LowIndependenceCount != expected.LowIndependenceCount ||
		report.HighContradictionCount != expected.HighContradictionCount ||
		report.VerifierStaleCount != expected.VerifierStaleCount ||
		report.HighUncertaintyCount != expected.HighUncertaintyCount {
		t.Fatalf("expected rpc aggregate diagnostics to mirror storage report, rpc=%+v expected=%+v", report, expected)
	}
	if report.LowIndependenceCount == 0 || report.HighContradictionCount == 0 || report.VerifierStaleCount == 0 || report.HighUncertaintyCount == 0 {
		t.Fatalf("expected rpc report to surface bounded aggregate diagnostics, got %+v", report)
	}
}
