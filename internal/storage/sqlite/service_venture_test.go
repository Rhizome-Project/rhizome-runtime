package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestServiceVentureLoopPersistsAndCoordinatesAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rhizome-service-venture.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	const (
		workspaceID = "ws-service-venture-reopen"
		projectID   = "project-service-venture-reopen"
		actorID     = "agent-portfolio"
	)
	seedServiceVentureProject(t, ctx, store, workspaceID, projectID, actorID)
	direction, candidate, run := seedServiceVentureRun(t, ctx, store, workspaceID, projectID, actorID, 5_000)

	grant, _, err := store.UpsertServiceApprovalGrantWithEvent(ctx, sqlite.ServiceApprovalGrantInput{
		GrantID:               "grant-service-ads",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		GrantType:             "paid_resource",
		ScopeJSON:             `{"provider":"mock-ads","cap_micros":5000}`,
		ApprovalRef:           "operator.approval.service-ads",
		Status:                sqlite.ServiceApprovalStatusApproved,
		ApprovedBy:            "operator-a",
		ActorID:               "operator-a",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.approval.grant", "server_rpc", workspaceID, "human", "operator-a"),
		PromptContextSurface:  "service.approval.grant",
	})
	if err != nil {
		t.Fatalf("approval grant: %v", err)
	}
	seedServiceEvidenceDocs(t, ctx, store, workspaceID, actorID,
		"evidence.deploy.public-url",
		"evidence.analytics.public-url",
		"evidence.spend.mock-ads",
		"evidence.revenue.mock-analytics",
	)
	resource, _, err := store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:             "res-mock-ads",
		WorkspaceID:            workspaceID,
		RunID:                  run.RunID,
		Provider:               "mock-ads",
		ResourceType:           "campaign",
		ResourceRef:            "campaign/subpixel-001",
		CredentialVaultEntryID: "vault/google-ads/test-ref",
		ApprovalGrantID:        grant.GrantID,
		Paid:                   true,
		CostCapMicros:          5_000,
		Status:                 sqlite.ServiceResourceStatusActive,
		ActorID:                actorID,
		ActorType:              "agent",
		PromptContextEnvelope:  sqlite.BuildServiceVenturePromptContextEnvelope("service.resource.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:   "service.resource.record",
	})
	if err != nil {
		t.Fatalf("provider resource: %v", err)
	}
	if _, _, err := store.RecordServiceSpendReceiptWithEvent(ctx, sqlite.ServiceSpendReceiptInput{
		ReceiptID:             "spend-mock-ads-1",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		ProviderResourceID:    resource.ResourceID,
		LedgerEntryID:         seedServiceBudgetSpendEntry(t, ctx, store, workspaceID, actorID, run, "ledger-spend-mock-ads-1", 1_500),
		AmountMicros:          1_500,
		Currency:              "USD",
		EvidenceRef:           "evidence.spend.mock-ads",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.spend.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.spend.record",
	}); err != nil {
		t.Fatalf("spend receipt: %v", err)
	}
	if _, _, err := store.RecordServiceRevenueObservationWithEvent(ctx, sqlite.ServiceRevenueObservationInput{
		ObservationID:         "revenue-early-interest",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		AmountMicros:          0,
		Currency:              "USD",
		Source:                "mock-analytics",
		EvidenceRef:           "evidence.revenue.mock-analytics",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.revenue.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.revenue.record",
	}); err != nil {
		t.Fatalf("revenue observation: %v", err)
	}
	outcomeInput := sqlite.ServiceOutcomeInput{
		OutcomeID:             "outcome-continue",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		PublicURL:             "https://example.com/subpixel-art",
		DeployHealthStatus:    sqlite.ServiceDeployHealthPass,
		DeployEvidenceRef:     "evidence.deploy.public-url",
		AnalyticsJSON:         `{"visits":42,"downloads":7}`,
		AnalyticsEvidenceRef:  "evidence.analytics.public-url",
		SpendMicros:           1_500,
		SpendEvidenceRef:      "evidence.spend.mock-ads",
		RevenueMicros:         0,
		RevenueEvidenceRef:    "evidence.revenue.mock-analytics",
		QualityScore:          82,
		Decision:              sqlite.ServiceOutcomeDecisionContinue,
		DecisionReason:        "Public deploy is healthy and early usage is real enough to continue.",
		EvidenceRefsJSON:      `["evidence.deploy.public-url","evidence.analytics.public-url","evidence.spend.mock-ads"]`,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.outcome.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.outcome.record",
	}
	outcome, _, err := store.RecordServiceOutcomeWithEvent(ctx, outcomeInput)
	if err != nil {
		t.Fatalf("record outcome: %v", err)
	}
	if outcome.Decision != sqlite.ServiceOutcomeDecisionContinue {
		t.Fatalf("unexpected outcome %+v", outcome)
	}
	replayedOutcome, replayEvent, err := store.RecordServiceOutcomeWithEvent(ctx, outcomeInput)
	if err != nil {
		t.Fatalf("replay outcome: %v", err)
	}
	if replayedOutcome.OutcomeID != outcome.OutcomeID || replayEvent.EventID != "" {
		t.Fatalf("expected idempotent outcome replay without new event, got outcome=%+v event=%+v", replayedOutcome, replayEvent)
	}
	measuring, err := store.GetServiceRun(ctx, workspaceID, run.RunID)
	if err != nil {
		t.Fatalf("get measured run: %v", err)
	}
	if measuring.Status != sqlite.ServiceRunStatusMeasuring || measuring.PublicURL != "https://example.com/subpixel-art" {
		t.Fatalf("expected measuring run with public URL, got %+v", measuring)
	}
	completed, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 run.RunID,
		WorkspaceID:           workspaceID,
		CandidateID:           candidate.CandidateID,
		ProjectID:             projectID,
		Title:                 measuring.Title,
		DeployTarget:          measuring.DeployTarget,
		PublicURL:             measuring.PublicURL,
		HealthCheckURL:        measuring.HealthCheckURL,
		BudgetAccountID:       measuring.BudgetAccountID,
		BudgetCapMicros:       measuring.BudgetCapMicros,
		CredentialPolicy:      measuring.CredentialPolicy,
		Status:                sqlite.ServiceRunStatusCompleted,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.update", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.run.update",
	})
	if err != nil {
		t.Fatalf("complete measured run: %v", err)
	}
	if completed.Status != sqlite.ServiceRunStatusCompleted {
		t.Fatalf("expected completed run, got %+v", completed)
	}
	if _, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 run.RunID,
		WorkspaceID:           workspaceID,
		CandidateID:           candidate.CandidateID,
		ProjectID:             projectID,
		Title:                 "Mutated after completion",
		DeployTarget:          "public-web",
		PublicURL:             completed.PublicURL,
		BudgetAccountID:       completed.BudgetAccountID,
		BudgetCapMicros:       completed.BudgetCapMicros,
		CredentialPolicy:      completed.CredentialPolicy,
		Status:                completed.Status,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.update", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.run.update",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected terminal service run mutation rejection, got %v", err)
	}
	if _, _, err := store.RecordServiceRevenueObservationWithEvent(ctx, sqlite.ServiceRevenueObservationInput{
		ObservationID:         "revenue-after-terminal",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		Source:                "mock-analytics",
		EvidenceRef:           "evidence.revenue.mock-analytics",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.revenue.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.revenue.record",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected terminal service run append rejection, got %v", err)
	}
	projectCoordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("project coordination: %v", err)
	}
	if len(projectCoordination.ServiceRuns) != 1 || projectCoordination.ServiceRuns[0].RunID != run.RunID {
		t.Fatalf("expected service run in project coordination, got %+v", projectCoordination.ServiceRuns)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.ApplyMigrations(ctx); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	coordination, err := reopened.GetServiceCoordination(ctx, workspaceID, run.RunID)
	if err != nil {
		t.Fatalf("service coordination after reopen: %v", err)
	}
	if coordination.Direction.DirectionID != direction.DirectionID || coordination.Candidate.CandidateID != candidate.CandidateID || coordination.Run.Status != sqlite.ServiceRunStatusCompleted {
		t.Fatalf("unexpected reopened coordination %+v", coordination)
	}
	if len(coordination.Outcomes) != 1 || len(coordination.ProviderResources) != 1 || len(coordination.SpendReceipts) != 1 || coordination.Project == nil {
		t.Fatalf("expected full service coordination packet after reopen, got %+v", coordination)
	}
}

func TestServiceOutcomeCannotContinueWithoutPublicEvidence(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-service-outcome-gate"
		projectID   = "project-service-outcome-gate"
		actorID     = "agent-qa"
	)
	seedServiceVentureProject(t, ctx, store, workspaceID, projectID, actorID)
	_, _, run := seedServiceVentureRun(t, ctx, store, workspaceID, projectID, actorID, 2_000)

	_, _, err := store.RecordServiceOutcomeWithEvent(ctx, sqlite.ServiceOutcomeInput{
		OutcomeID:             "outcome-missing-public-evidence",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		PublicURL:             "http://localhost:5173",
		DeployHealthStatus:    sqlite.ServiceDeployHealthPass,
		AnalyticsJSON:         `{"visits":1}`,
		Decision:              sqlite.ServiceOutcomeDecisionContinue,
		DecisionReason:        "Looks fine from local smoke.",
		EvidenceRefsJSON:      `["local.screenshot"]`,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.outcome.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.outcome.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected service venture invalid for local/incomplete continue evidence, got %v", err)
	}
	freshRun, getErr := store.GetServiceRun(ctx, workspaceID, run.RunID)
	if getErr != nil {
		t.Fatalf("get run after rejected outcome: %v", getErr)
	}
	if freshRun.Status != sqlite.ServiceRunStatusActive {
		t.Fatalf("rejected outcome should not complete run, got %+v", freshRun)
	}
}

func TestServiceResourceAndSpendGovernanceFailClosed(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-service-resource-governance"
		projectID   = "project-service-resource-governance"
		actorID     = "agent-governance"
	)
	seedServiceVentureProject(t, ctx, store, workspaceID, projectID, actorID)
	_, _, run := seedServiceVentureRun(t, ctx, store, workspaceID, projectID, actorID, 1_000)
	seedServiceEvidenceDocs(t, ctx, store, workspaceID, actorID,
		"evidence.spend.under-cap",
		"evidence.spend.over-cap",
	)

	_, _, err := store.UpsertServiceApprovalGrantWithEvent(ctx, sqlite.ServiceApprovalGrantInput{
		GrantID:               "grant-agent-self-approval",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		GrantType:             "paid_resource",
		ScopeJSON:             `{"provider":"mock-ads","cap_micros":1000}`,
		ApprovalRef:           "agent.self-approval",
		Status:                sqlite.ServiceApprovalStatusApproved,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.approval.grant", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.approval.grant",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected agent self-approval rejection, got %v", err)
	}

	scopeGrant, _, err := store.UpsertServiceApprovalGrantWithEvent(ctx, sqlite.ServiceApprovalGrantInput{
		GrantID:               "grant-scope-mock-ads",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		GrantType:             "paid_resource",
		ScopeJSON:             `{"provider":"mock-ads","cap_micros":100}`,
		ApprovalRef:           "operator.approval.scope",
		Status:                sqlite.ServiceApprovalStatusApproved,
		ApprovedBy:            "operator-a",
		ActorID:               "operator-a",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.approval.grant", "server_rpc", workspaceID, "human", "operator-a"),
		PromptContextSurface:  "service.approval.grant",
	})
	if err != nil {
		t.Fatalf("scope approval grant: %v", err)
	}
	_, _, err = store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:      "res-scope-mismatch",
		WorkspaceID:     workspaceID,
		RunID:           run.RunID,
		Provider:        "other-ads",
		ResourceType:    "campaign",
		ApprovalGrantID: scopeGrant.GrantID,
		Paid:            true,
		CostCapMicros:   50,
		Status:          sqlite.ServiceResourceStatusActive,
		ActorID:         actorID,
		ActorType:       "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope(
			"service.resource.record", "server_rpc", workspaceID, "agent", actorID,
		),
		PromptContextSurface: "service.resource.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected approval scope provider mismatch rejection, got %v", err)
	}

	_, _, err = store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:            "res-paid-without-grant",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		Provider:              "mock-ads",
		ResourceType:          "campaign",
		Paid:                  true,
		Status:                sqlite.ServiceResourceStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.resource.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.resource.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected paid resource without grant to fail closed, got %v", err)
	}

	_, _, err = store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:             "res-secret-material",
		WorkspaceID:            workspaceID,
		RunID:                  run.RunID,
		Provider:               "openai",
		ResourceType:           "api-key",
		CredentialVaultEntryID: "sk-should-not-be-stored-here",
		Status:                 sqlite.ServiceResourceStatusPendingApproval,
		ActorID:                actorID,
		ActorType:              "agent",
		PromptContextEnvelope:  sqlite.BuildServiceVenturePromptContextEnvelope("service.resource.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:   "service.resource.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected credential material rejection, got %v", err)
	}

	freeTierRun, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 "run-free-tier",
		WorkspaceID:           workspaceID,
		CandidateID:           run.CandidateID,
		ProjectID:             projectID,
		Title:                 "Free tier only run",
		DeployTarget:          "public-web",
		BudgetAccountID:       run.BudgetAccountID,
		BudgetCapMicros:       1_000,
		CredentialPolicy:      sqlite.ServiceCredentialPolicyFreeTierOnly,
		Status:                sqlite.ServiceRunStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.start", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.run.start",
	})
	if err != nil {
		t.Fatalf("free tier service run: %v", err)
	}
	_, _, err = store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:             "res-free-tier-paid-pending",
		WorkspaceID:            workspaceID,
		RunID:                  freeTierRun.RunID,
		Provider:               "mock-ads",
		ResourceType:           "campaign",
		CredentialVaultEntryID: "vault/mock-ads/test-ref",
		Paid:                   true,
		Status:                 sqlite.ServiceResourceStatusPendingApproval,
		ActorID:                actorID,
		ActorType:              "agent",
		PromptContextEnvelope:  sqlite.BuildServiceVenturePromptContextEnvelope("service.resource.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:   "service.resource.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected FREE_TIER_ONLY paid/credentialed pending resource rejection, got %v", err)
	}

	_, _, err = store.RecordServiceOutcomeWithEvent(ctx, sqlite.ServiceOutcomeInput{
		OutcomeID:             "outcome-blocked-missing-evidence",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		Decision:              sqlite.ServiceOutcomeDecisionBlocked,
		DecisionReason:        "Need more evidence.",
		EvidenceRefsJSON:      `["missing.service.evidence"]`,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.outcome.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.outcome.record",
	})
	if !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected blocked outcome with unresolved evidence rejection, got %v", err)
	}

	if _, _, err := store.RecordServiceSpendReceiptWithEvent(ctx, sqlite.ServiceSpendReceiptInput{
		ReceiptID:             "spend-missing-ledger",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		AmountMicros:          1,
		EvidenceRef:           "evidence.spend.under-cap",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.spend.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.spend.record",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected positive spend without ledger entry to fail, got %v", err)
	}

	if _, _, err := store.RecordServiceSpendReceiptWithEvent(ctx, sqlite.ServiceSpendReceiptInput{
		ReceiptID:             "spend-under-cap",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		LedgerEntryID:         seedServiceBudgetSpendEntry(t, ctx, store, workspaceID, actorID, run, "ledger-spend-under-cap", 700),
		AmountMicros:          700,
		EvidenceRef:           "evidence.spend.under-cap",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.spend.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.spend.record",
	}); err != nil {
		t.Fatalf("record spend under cap: %v", err)
	}
	_, _, err = store.RecordServiceSpendReceiptWithEvent(ctx, sqlite.ServiceSpendReceiptInput{
		ReceiptID:             "spend-over-cap",
		WorkspaceID:           workspaceID,
		RunID:                 run.RunID,
		LedgerEntryID:         seedServiceBudgetSpendEntry(t, ctx, store, workspaceID, actorID, run, "ledger-spend-over-cap", 400),
		AmountMicros:          400,
		EvidenceRef:           "evidence.spend.over-cap",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.spend.record", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.spend.record",
	})
	if !errors.Is(err, sqlite.ErrBudgetExceeded) {
		t.Fatalf("expected spend over cap rejection, got %v", err)
	}
}

func TestServiceVentureRejectsCrossWorkspaceEntityIdentityCollisions(t *testing.T) {
	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	const actorID = "agent-portfolio"
	seedServiceVentureProject(t, ctx, store, "ws-service-identity-a", "project-service-identity-a", actorID)
	seedServiceVentureProject(t, ctx, store, "ws-service-identity-b", "project-service-identity-b", actorID)

	directionA, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           "direction-shared-id",
		IdempotencyKey:        "idem-direction-shared",
		WorkspaceID:           "ws-service-identity-a",
		Title:                 "Direction A",
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", "ws-service-identity-a", "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	})
	if err != nil {
		t.Fatalf("direction A: %v", err)
	}
	if _, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           directionA.DirectionID,
		IdempotencyKey:        "idem-direction-b",
		WorkspaceID:           "ws-service-identity-b",
		Title:                 "Direction B collision",
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected cross-workspace direction_id collision rejection, got %v", err)
	}
	directionBWithSameIdempotency, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           "direction-b-other-id",
		IdempotencyKey:        directionA.IdempotencyKey,
		WorkspaceID:           "ws-service-identity-b",
		Title:                 "Direction B idempotency collision",
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	})
	if err != nil {
		t.Fatalf("same idempotency key in another workspace should be allowed, got %v", err)
	}
	if directionBWithSameIdempotency.WorkspaceID != "ws-service-identity-b" {
		t.Fatalf("same idempotency key in another workspace returned wrong row: %+v", directionBWithSameIdempotency)
	}

	directionB, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           "direction-identity-b",
		IdempotencyKey:        "idem-direction-identity-b",
		WorkspaceID:           "ws-service-identity-b",
		Title:                 "Direction B",
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	})
	if err != nil {
		t.Fatalf("direction B: %v", err)
	}
	candidateA, _, err := store.UpsertServiceCandidateWithEvent(ctx, sqlite.ServiceCandidateInput{
		CandidateID:           "candidate-shared-id",
		IdempotencyKey:        "idem-candidate-shared",
		WorkspaceID:           "ws-service-identity-a",
		DirectionID:           directionA.DirectionID,
		Title:                 "Candidate A",
		Status:                sqlite.ServiceCandidateStatusSelected,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.candidate.upsert", "server_rpc", "ws-service-identity-a", "agent", actorID),
		PromptContextSurface:  "service.candidate.upsert",
	})
	if err != nil {
		t.Fatalf("candidate A: %v", err)
	}
	if _, _, err := store.UpsertServiceCandidateWithEvent(ctx, sqlite.ServiceCandidateInput{
		CandidateID:           candidateA.CandidateID,
		IdempotencyKey:        "idem-candidate-b",
		WorkspaceID:           "ws-service-identity-b",
		DirectionID:           directionB.DirectionID,
		Title:                 "Candidate B collision",
		Status:                sqlite.ServiceCandidateStatusSelected,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.candidate.upsert", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.candidate.upsert",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected cross-workspace candidate_id collision rejection, got %v", err)
	}

	candidateB, _, err := store.UpsertServiceCandidateWithEvent(ctx, sqlite.ServiceCandidateInput{
		CandidateID:           "candidate-identity-b",
		IdempotencyKey:        "idem-candidate-identity-b",
		WorkspaceID:           "ws-service-identity-b",
		DirectionID:           directionB.DirectionID,
		Title:                 "Candidate B",
		Status:                sqlite.ServiceCandidateStatusSelected,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.candidate.upsert", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.candidate.upsert",
	})
	if err != nil {
		t.Fatalf("candidate B: %v", err)
	}
	runA, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 "run-shared-id",
		IdempotencyKey:        "idem-run-shared",
		WorkspaceID:           "ws-service-identity-a",
		CandidateID:           candidateA.CandidateID,
		ProjectID:             "project-service-identity-a",
		Title:                 "Run A",
		Status:                sqlite.ServiceRunStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.start", "server_rpc", "ws-service-identity-a", "agent", actorID),
		PromptContextSurface:  "service.run.start",
	})
	if err != nil {
		t.Fatalf("run A: %v", err)
	}
	if _, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 runA.RunID,
		IdempotencyKey:        "idem-run-b",
		WorkspaceID:           "ws-service-identity-b",
		CandidateID:           candidateB.CandidateID,
		ProjectID:             "project-service-identity-b",
		Title:                 "Run B collision",
		Status:                sqlite.ServiceRunStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.start", "server_rpc", "ws-service-identity-b", "agent", actorID),
		PromptContextSurface:  "service.run.start",
	}); !errors.Is(err, sqlite.ErrServiceVentureInvalid) {
		t.Fatalf("expected cross-workspace run_id collision rejection, got %v", err)
	}
}

func TestServiceVentureGeneratedIDIdempotencyRetriesReuseExistingEntity(t *testing.T) {
	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	const (
		workspaceID = "ws-service-generated-id"
		projectID   = "project-service-generated-id"
		actorID     = "agent-portfolio"
	)
	seedServiceVentureProject(t, ctx, store, workspaceID, projectID, actorID)
	directionInput := sqlite.ServiceDirectionBriefInput{
		IdempotencyKey:        "idem-generated-direction",
		WorkspaceID:           workspaceID,
		Title:                 "Generated direction",
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	}
	direction1, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, directionInput)
	if err != nil {
		t.Fatalf("direction first create: %v", err)
	}
	direction2, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, directionInput)
	if err != nil {
		t.Fatalf("direction idempotent retry: %v", err)
	}
	if direction1.DirectionID != direction2.DirectionID {
		t.Fatalf("expected generated direction id retry to reuse %s, got %s", direction1.DirectionID, direction2.DirectionID)
	}

	candidateInput := sqlite.ServiceCandidateInput{
		IdempotencyKey:        "idem-generated-candidate",
		WorkspaceID:           workspaceID,
		DirectionID:           direction1.DirectionID,
		Title:                 "Generated candidate",
		Status:                sqlite.ServiceCandidateStatusSelected,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.candidate.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.candidate.upsert",
	}
	candidate1, _, err := store.UpsertServiceCandidateWithEvent(ctx, candidateInput)
	if err != nil {
		t.Fatalf("candidate first create: %v", err)
	}
	candidate2, _, err := store.UpsertServiceCandidateWithEvent(ctx, candidateInput)
	if err != nil {
		t.Fatalf("candidate idempotent retry: %v", err)
	}
	if candidate1.CandidateID != candidate2.CandidateID {
		t.Fatalf("expected generated candidate id retry to reuse %s, got %s", candidate1.CandidateID, candidate2.CandidateID)
	}

	runInput := sqlite.ServiceRunInput{
		IdempotencyKey:        "idem-generated-run",
		WorkspaceID:           workspaceID,
		CandidateID:           candidate1.CandidateID,
		ProjectID:             projectID,
		Title:                 "Generated run",
		Status:                sqlite.ServiceRunStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.start", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.run.start",
	}
	run1, _, err := store.UpsertServiceRunWithEvent(ctx, runInput)
	if err != nil {
		t.Fatalf("run first create: %v", err)
	}
	run2, _, err := store.UpsertServiceRunWithEvent(ctx, runInput)
	if err != nil {
		t.Fatalf("run idempotent retry: %v", err)
	}
	if run1.RunID != run2.RunID {
		t.Fatalf("expected generated run id retry to reuse %s, got %s", run1.RunID, run2.RunID)
	}
}

func seedServiceVentureProject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure workspace authority: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, _, err := store.CreateProjectWithEvent(ctx, sqlite.ProjectCreateInput{
		ProjectID:             projectID,
		WorkspaceID:           workspaceID,
		Title:                 projectID,
		Description:           "Service venture implementation project.",
		CreatedBy:             actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.create", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.create",
	}); err != nil {
		t.Fatalf("create project with event: %v", err)
	}
}

func seedServiceVentureRun(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string, budgetCapMicros int64) (sqlite.ServiceDirectionBriefRecord, sqlite.ServiceCandidateRecord, sqlite.ServiceRunRecord) {
	t.Helper()
	direction, _, err := store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           "direction-service-tools",
		WorkspaceID:           workspaceID,
		Title:                 "Ship small image tools",
		Description:           "Find, build, deploy, and measure small web utilities.",
		ConstraintsJSON:       `{"deployment":"public-web","ads":"allowed-with-approval"}`,
		BudgetCapMicros:       budgetCapMicros,
		Status:                sqlite.ServiceDirectionStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.direction.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.direction.upsert",
	})
	if err != nil {
		t.Fatalf("upsert service direction: %v", err)
	}
	candidate, _, err := store.UpsertServiceCandidateWithEvent(ctx, sqlite.ServiceCandidateInput{
		CandidateID:           "candidate-subpixel-art",
		WorkspaceID:           workspaceID,
		DirectionID:           direction.DirectionID,
		Title:                 "Subpixel art converter",
		TargetUser:            "Designers and display enthusiasts",
		UserPain:              "Previewing subpixel conversions is cumbersome.",
		SolutionSummary:       "Browser converter with deterministic PNG output.",
		Distribution:          "SEO and small paid experiments",
		Monetization:          "Ads after public deployment",
		ImplementationSize:    "small",
		RiskLevel:             "medium",
		Score:                 91,
		EvidencePlanJSON:      `{"deploy":"public-url","analytics":"visits-downloads","spend":"receipts"}`,
		Status:                sqlite.ServiceCandidateStatusSelected,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.candidate.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.candidate.upsert",
	})
	if err != nil {
		t.Fatalf("upsert service candidate: %v", err)
	}
	accountID := "budget-" + workspaceID
	if _, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     accountID,
		PrincipalType: "service_run",
		PrincipalID:   projectID,
		WorkspaceID:   workspaceID,
		Currency:      "USD",
		LimitMicros:   100_000,
		Status:        sqlite.BudgetAccountStatusActive,
	}); err != nil {
		t.Fatalf("ensure service budget account: %v", err)
	}
	run, _, err := store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 "run-subpixel-art",
		WorkspaceID:           workspaceID,
		CandidateID:           candidate.CandidateID,
		ProjectID:             projectID,
		Title:                 "Subpixel art launch",
		DeployTarget:          "public-web",
		BudgetAccountID:       accountID,
		BudgetCapMicros:       budgetCapMicros,
		CredentialPolicy:      sqlite.ServiceCredentialPolicyApproved,
		Status:                sqlite.ServiceRunStatusActive,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildServiceVenturePromptContextEnvelope("service.run.start", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "service.run.start",
	})
	if err != nil {
		t.Fatalf("upsert service run: %v", err)
	}
	return direction, candidate, run
}

func seedServiceEvidenceDocs(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, refs ...string) {
	t.Helper()
	for _, ref := range refs {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      ref,
			Title:       ref,
			Content:     "rhizome_service_evidence_v1\nkind: test\nref: " + ref,
			UpdatedBy:   actorID,
		}); err != nil {
			t.Fatalf("seed service evidence doc %s: %v", ref, err)
		}
	}
}

func seedServiceBudgetSpendEntry(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, run sqlite.ServiceRunRecord, entryID string, amountMicros int64) string {
	t.Helper()
	entryID = strings.TrimSpace(entryID)
	if entryID == "" {
		t.Fatalf("entryID is required")
	}
	reservationID := entryID + "-reservation"
	scope := sqlite.BudgetReservationInput{
		ReservationID:  reservationID,
		IdempotencyKey: reservationID,
		AccountID:      run.BudgetAccountID,
		WorkspaceID:    workspaceID,
		AgentID:        actorID,
		TaskID:         "service-venture-test",
		RunID:          run.RunID,
		ProviderID:     "service-venture",
		Model:          "external-spend",
		AmountMicros:   amountMicros,
		Reason:         "service venture test spend reservation",
	}
	if _, err := store.ReserveBudget(ctx, scope); err != nil {
		t.Fatalf("reserve service budget spend %s: %v", entryID, err)
	}
	if _, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        entryID,
		IdempotencyKey: entryID,
		AccountID:      run.BudgetAccountID,
		ReservationID:  reservationID,
		WorkspaceID:    workspaceID,
		AgentID:        actorID,
		TaskID:         "service-venture-test",
		RunID:          run.RunID,
		ProviderID:     "service-venture",
		Model:          "external-spend",
		AmountMicros:   amountMicros,
		Reason:         "service venture test spend capture",
	}); err != nil {
		t.Fatalf("capture service budget spend %s: %v", entryID, err)
	}
	return entryID
}
