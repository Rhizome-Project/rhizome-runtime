package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestServiceVentureRPCLifecycleAndCoordination(t *testing.T) {
	store := newServerTestStore(t)
	defer func() { _ = store.Close() }()
	h := NewHandler(store)
	ctx := context.Background()
	const (
		workspaceID = "ws-service-venture-rpc"
		projectID   = "project-service-venture-rpc"
		actorID     = "agent-portfolio"
		accountID   = "budget-ws-service-venture-rpc"
	)
	seedServiceVentureRPCProject(t, ctx, store, workspaceID, projectID, actorID)
	seedServiceVentureRPCBudgetAccount(t, ctx, store, workspaceID, projectID, accountID)
	rpcCtx := testAuthContext(workspaceID, "agent", actorID)

	if _, rpcErr := h.serviceDirectionUpsert(rpcCtx, serviceRaw(t, serviceDirectionUpsertParams{
		WorkspaceID:     workspaceID,
		DirectionID:     "direction-rpc-tools",
		ActorID:         actorID,
		Title:           "Small public web tools",
		ConstraintsJSON: `{"public_deploy":true}`,
		Status:          sqlite.ServiceDirectionStatusActive,
	})); rpcErr != nil {
		t.Fatalf("service.direction.upsert: %+v", rpcErr)
	}
	if _, rpcErr := h.serviceCandidateUpsert(rpcCtx, serviceRaw(t, serviceCandidateUpsertParams{
		WorkspaceID:      workspaceID,
		CandidateID:      "candidate-rpc-subpixel",
		ActorID:          actorID,
		DirectionID:      "direction-rpc-tools",
		Title:            "Subpixel art converter",
		EvidencePlanJSON: `{"deploy":"public","analytics":"visits","spend":"receipts"}`,
		Status:           sqlite.ServiceCandidateStatusSelected,
		Score:            88,
	})); rpcErr != nil {
		t.Fatalf("service.candidate.upsert: %+v", rpcErr)
	}
	if _, rpcErr := h.serviceCandidateUpsert(rpcCtx, serviceRaw(t, serviceCandidateUpsertParams{
		WorkspaceID:      workspaceID,
		CandidateID:      "candidate-other-existing",
		ActorID:          actorID,
		DirectionID:      "direction-rpc-tools",
		Title:            "Other converter",
		EvidencePlanJSON: `{"deploy":"public"}`,
		Status:           sqlite.ServiceCandidateStatusSelected,
		Score:            10,
	})); rpcErr != nil {
		t.Fatalf("service.candidate.upsert other: %+v", rpcErr)
	}
	runResult, rpcErr := h.serviceRunStart(rpcCtx, serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID:      workspaceID,
		RunID:            "run-rpc-subpixel",
		ActorID:          actorID,
		CandidateID:      "candidate-rpc-subpixel",
		ProjectID:        projectID,
		Title:            "Subpixel launch",
		DeployTarget:     "vercel",
		BudgetAccountID:  accountID,
		BudgetCapMicros:  2_000,
		CredentialPolicy: sqlite.ServiceCredentialPolicyApproved,
		Status:           sqlite.ServiceRunStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("service.run.start: %+v", rpcErr)
	}
	run := runResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	if run.ProjectID != projectID || run.Status != sqlite.ServiceRunStatusActive {
		t.Fatalf("unexpected run %+v", run)
	}
	statusOnlyResult, rpcErr := h.serviceRunUpdate(rpcCtx, serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID: workspaceID,
		RunID:       run.RunID,
		ActorID:     actorID,
		Status:      sqlite.ServiceRunStatusDeployed,
		PublicURL:   "https://example.com/status-only",
	}))
	if rpcErr != nil {
		t.Fatalf("service.run.update status-only: %+v", rpcErr)
	}
	run = statusOnlyResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	if run.CandidateID != "candidate-rpc-subpixel" || run.ProjectID != projectID || run.Status != sqlite.ServiceRunStatusDeployed || run.PublicURL != "https://example.com/status-only" {
		t.Fatalf("status-only update did not preserve immutable run identity: %+v", run)
	}
	if _, rpcErr := h.serviceRunUpdate(rpcCtx, serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID: workspaceID,
		RunID:       run.RunID,
		ActorID:     actorID,
		ProjectID:   "project-wrong-restatement",
		Status:      sqlite.ServiceRunStatusDeployed,
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected mismatched project_id restatement rejection, got %+v", rpcErr)
	}
	if _, rpcErr := h.serviceRunUpdate(testAuthContext(workspaceID, "agent", "agent-rogue"), serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID:      workspaceID,
		RunID:            run.RunID,
		ActorID:          "agent-rogue",
		CandidateID:      "candidate-rpc-subpixel",
		ProjectID:        projectID,
		Title:            "Rogue update",
		DeployTarget:     "vercel",
		BudgetAccountID:  accountID,
		BudgetCapMicros:  2_000,
		CredentialPolicy: sqlite.ServiceCredentialPolicyApproved,
		Status:           sqlite.ServiceRunStatusActive,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected rogue service.run.update permission denied, got %+v", rpcErr)
	}

	if _, rpcErr := h.serviceOutcomeRecord(rpcCtx, serviceRaw(t, serviceOutcomeRecordParams{
		WorkspaceID:        workspaceID,
		OutcomeID:          "outcome-localhost",
		ActorID:            actorID,
		RunID:              run.RunID,
		PublicURL:          "http://localhost:3000",
		DeployHealthStatus: sqlite.ServiceDeployHealthPass,
		AnalyticsJSON:      `{"visits":1}`,
		Decision:           sqlite.ServiceOutcomeDecisionContinue,
		DecisionReason:     "Local smoke only.",
		EvidenceRefsJSON:   `["local.screenshot"]`,
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for localhost continue outcome, got %+v", rpcErr)
	}
	if _, rpcErr := h.serviceRunUpdate(rpcCtx, serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID:      workspaceID,
		RunID:            run.RunID,
		ActorID:          actorID,
		CandidateID:      "candidate-other-existing",
		ProjectID:        projectID,
		Title:            "Misleading candidate update",
		DeployTarget:     "vercel",
		BudgetAccountID:  accountID,
		BudgetCapMicros:  2_000,
		CredentialPolicy: sqlite.ServiceCredentialPolicyApproved,
		Status:           sqlite.ServiceRunStatusActive,
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected immutable service.run.update candidate rejection, got %+v", rpcErr)
	}

	operatorCtx := testAuthContext(workspaceID, "human", "operator-a")
	grantResult, rpcErr := h.serviceApprovalGrant(operatorCtx, serviceRaw(t, serviceApprovalGrantParams{
		WorkspaceID: workspaceID,
		GrantID:     "grant-rpc-ads",
		ActorID:     "operator-a",
		RunID:       run.RunID,
		GrantType:   "paid_resource",
		ScopeJSON:   `{"provider":"mock-ads","cap_micros":2000}`,
		ApprovalRef: "operator.approval.rpc-ads",
		Status:      sqlite.ServiceApprovalStatusApproved,
		ApprovedBy:  "operator-a",
	}))
	if rpcErr != nil {
		t.Fatalf("service.approval.grant: %+v", rpcErr)
	}
	grant := grantResult.(map[string]any)["approval_grant"].(sqlite.ServiceApprovalGrantRecord)
	seedServiceVentureRPCEvidenceDocs(t, ctx, store, workspaceID, actorID, "evidence.rpc.spend")
	if _, rpcErr := h.serviceResourceRecord(rpcCtx, serviceRaw(t, serviceResourceRecordParams{
		WorkspaceID:            workspaceID,
		ResourceID:             "resource-rpc-ads",
		ActorID:                actorID,
		RunID:                  run.RunID,
		Provider:               "mock-ads",
		ResourceType:           "campaign",
		ResourceRef:            "campaign/rpc-subpixel",
		CredentialVaultEntryID: "vault/mock-ads/test",
		ApprovalGrantID:        grant.GrantID,
		Paid:                   true,
		CostCapMicros:          2_000,
		Status:                 sqlite.ServiceResourceStatusActive,
	})); rpcErr != nil {
		t.Fatalf("service.resource.record: %+v", rpcErr)
	}
	if _, rpcErr := h.serviceSpendRecord(rpcCtx, serviceRaw(t, serviceSpendRecordParams{
		WorkspaceID:        workspaceID,
		ReceiptID:          "spend-rpc-1",
		ActorID:            actorID,
		RunID:              run.RunID,
		LedgerEntryID:      seedServiceVentureRPCBudgetSpendEntry(t, ctx, store, workspaceID, actorID, run, "ledger-spend-rpc-1", 1_000),
		AmountMicros:       1_000,
		EvidenceRef:        "evidence.rpc.spend",
		ProviderResourceID: "resource-rpc-ads",
	})); rpcErr != nil {
		t.Fatalf("service.spend.record: %+v", rpcErr)
	}
	seedServiceVentureRPCEvidenceDocs(t, ctx, store, workspaceID, actorID,
		"evidence.rpc.deploy",
		"evidence.rpc.analytics",
	)
	outcomeResult, rpcErr := h.serviceOutcomeRecord(rpcCtx, serviceRaw(t, serviceOutcomeRecordParams{
		WorkspaceID:          workspaceID,
		OutcomeID:            "outcome-rpc-continue",
		ActorID:              actorID,
		RunID:                run.RunID,
		PublicURL:            "https://example.com/rpc-subpixel",
		DeployHealthStatus:   sqlite.ServiceDeployHealthPass,
		DeployEvidenceRef:    "evidence.rpc.deploy",
		AnalyticsJSON:        `{"visits":12,"downloads":2}`,
		AnalyticsEvidenceRef: "evidence.rpc.analytics",
		SpendMicros:          1_000,
		SpendEvidenceRef:     "evidence.rpc.spend",
		Decision:             sqlite.ServiceOutcomeDecisionContinue,
		DecisionReason:       "Public deploy and analytics are sufficient for a next iteration.",
		EvidenceRefsJSON:     `["evidence.rpc.deploy","evidence.rpc.analytics","evidence.rpc.spend"]`,
	}))
	if rpcErr != nil {
		t.Fatalf("service.outcome.record: %+v", rpcErr)
	}
	if got := outcomeResult.(map[string]any)["outcome"].(sqlite.ServiceOutcomeRecord).Decision; got != sqlite.ServiceOutcomeDecisionContinue {
		t.Fatalf("unexpected decision %q", got)
	}
	coordinationResult, rpcErr := h.serviceCoordinationGet(rpcCtx, serviceRaw(t, serviceCoordinationGetParams{
		WorkspaceID: workspaceID,
		RunID:       run.RunID,
	}))
	if rpcErr != nil {
		t.Fatalf("service.coordination.get: %+v", rpcErr)
	}
	coordination := coordinationResult.(map[string]any)["coordination"].(sqlite.ServiceCoordinationRecord)
	if coordination.Run.Status != sqlite.ServiceRunStatusMeasuring || coordination.Project == nil || len(coordination.Project.ServiceRuns) != 1 {
		t.Fatalf("unexpected service coordination %+v", coordination)
	}

	clearRunResult, rpcErr := h.serviceRunStart(rpcCtx, serviceRaw(t, serviceRunUpsertParams{
		WorkspaceID:      workspaceID,
		RunID:            "run-rpc-clearable",
		ActorID:          actorID,
		CandidateID:      "candidate-rpc-subpixel",
		ProjectID:        projectID,
		Title:            "Clearable launch metadata",
		DeployTarget:     "vercel",
		PublicURL:        "https://example.com/clearable",
		HealthCheckURL:   "https://example.com/clearable/health",
		BudgetAccountID:  accountID,
		BudgetCapMicros:  99,
		CredentialPolicy: sqlite.ServiceCredentialPolicyApproved,
		Status:           sqlite.ServiceRunStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("service.run.start clearable: %+v", rpcErr)
	}
	clearRun := clearRunResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	clearRunResult, rpcErr = h.serviceRunUpdate(rpcCtx, serviceRaw(t, map[string]any{
		"workspace_id":      workspaceID,
		"run_id":            clearRun.RunID,
		"actor_id":          actorID,
		"public_url":        "",
		"health_check_url":  "",
		"budget_account_id": "",
		"budget_cap_micros": 0,
		"credential_policy": sqlite.ServiceCredentialPolicyApproved,
		"status":            sqlite.ServiceRunStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("service.run.update clear metadata: %+v", rpcErr)
	}
	clearRun = clearRunResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	if clearRun.PublicURL != "" || clearRun.HealthCheckURL != "" || clearRun.BudgetAccountID != "" || clearRun.BudgetCapMicros != 0 {
		t.Fatalf("expected clearable metadata to reset, got %+v", clearRun)
	}
}

func TestServiceVentureRPCStartGeneratesRunIDAndReplaysIdempotency(t *testing.T) {
	store := newServerTestStore(t)
	defer func() { _ = store.Close() }()
	h := NewHandler(store)
	ctx := context.Background()
	const (
		workspaceID    = "ws-service-run-generated-rpc"
		projectID      = "project-service-run-generated-rpc"
		actorID        = "agent-portfolio"
		idempotencyKey = "idem-service-run-generated-rpc"
	)
	seedServiceVentureRPCProject(t, ctx, store, workspaceID, projectID, actorID)
	rpcCtx := testAuthContext(workspaceID, "agent", actorID)

	if _, rpcErr := h.serviceDirectionUpsert(rpcCtx, serviceRaw(t, serviceDirectionUpsertParams{
		WorkspaceID: workspaceID,
		DirectionID: "direction-generated-run",
		ActorID:     actorID,
		Title:       "Generated run direction",
		Status:      sqlite.ServiceDirectionStatusActive,
	})); rpcErr != nil {
		t.Fatalf("service.direction.upsert: %+v", rpcErr)
	}
	if _, rpcErr := h.serviceCandidateUpsert(rpcCtx, serviceRaw(t, serviceCandidateUpsertParams{
		WorkspaceID:      workspaceID,
		CandidateID:      "candidate-generated-run",
		ActorID:          actorID,
		DirectionID:      "direction-generated-run",
		Title:            "Generated run candidate",
		Status:           sqlite.ServiceCandidateStatusSelected,
		EvidencePlanJSON: `{"deploy":"public"}`,
		Score:            42,
	})); rpcErr != nil {
		t.Fatalf("service.candidate.upsert: %+v", rpcErr)
	}

	params := map[string]any{
		"workspace_id":      workspaceID,
		"idempotency_key":   idempotencyKey,
		"actor_id":          actorID,
		"candidate_id":      "candidate-generated-run",
		"project_id":        projectID,
		"title":             "Generated run launch",
		"deploy_target":     "vercel",
		"credential_policy": sqlite.ServiceCredentialPolicyApproved,
		"status":            sqlite.ServiceRunStatusActive,
	}
	firstResult, rpcErr := h.serviceRunStart(rpcCtx, serviceRaw(t, params))
	if rpcErr != nil {
		t.Fatalf("service.run.start generated id: %+v", rpcErr)
	}
	firstRun := firstResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	if firstRun.RunID == "" {
		t.Fatalf("expected generated run_id, got %+v", firstRun)
	}

	secondResult, rpcErr := h.serviceRunStart(rpcCtx, serviceRaw(t, params))
	if rpcErr != nil {
		t.Fatalf("service.run.start generated id replay: %+v", rpcErr)
	}
	secondRun := secondResult.(map[string]any)["run"].(sqlite.ServiceRunRecord)
	if secondRun.RunID != firstRun.RunID {
		t.Fatalf("expected idempotent replay to keep run_id %q, got %+v", firstRun.RunID, secondRun)
	}
}

func TestServiceVentureRPCRejectsActorMismatch(t *testing.T) {
	store := newServerTestStore(t)
	defer func() { _ = store.Close() }()
	h := NewHandler(store)
	ctx := context.Background()
	const workspaceID = "ws-service-venture-actor-mismatch"
	const actorID = "agent-a"
	seedServiceVentureRPCWorkspace(t, ctx, store, workspaceID, actorID)

	_, rpcErr := h.serviceDirectionUpsert(testAuthContext(workspaceID, "agent", actorID), serviceRaw(t, serviceDirectionUpsertParams{
		WorkspaceID: workspaceID,
		ActorID:     "agent-b",
		Title:       "Forged actor direction",
	}))
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for actor mismatch, got %+v", rpcErr)
	}
}

func seedServiceVentureRPCProject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	seedServiceVentureRPCWorkspace(t, ctx, store, workspaceID, actorID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:  workspaceID,
		AgentID:      actorID,
		OwnerUserID:  "owner-test",
		DisplayName:  actorID,
		Role:         "portfolio",
		Status:       "ACTIVE",
		Capabilities: []string{"service-venture"},
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, _, err := store.CreateProjectWithEvent(ctx, sqlite.ProjectCreateInput{
		ProjectID:             projectID,
		WorkspaceID:           workspaceID,
		Title:                 projectID,
		Description:           "Service venture RPC project.",
		CreatedBy:             actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.create", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.create",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               actorID,
		ActorType:             "agent",
		AgentID:               actorID,
		LeaseSeconds:          15 * 60,
		Summary:               "Service venture RPC test lead.",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim project lead: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               actorID,
		ActorType:             "agent",
		AgentID:               actorID,
		RoleType:              sqlite.ProjectRolePlanner,
		WriteScopeJSON:        `{}`,
		Summary:               "Service venture RPC test role.",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign project role: %v", err)
	}
}

func seedServiceVentureRPCWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string) {
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
		t.Fatalf("ensure authority: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
}

func seedServiceVentureRPCBudgetAccount(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, accountID string) {
	t.Helper()
	if _, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     accountID,
		PrincipalType: "project",
		PrincipalID:   projectID,
		WorkspaceID:   workspaceID,
		Currency:      "USD",
		LimitMicros:   100_000,
		Status:        sqlite.BudgetAccountStatusActive,
	}); err != nil {
		t.Fatalf("ensure service RPC budget account: %v", err)
	}
}

func seedServiceVentureRPCEvidenceDocs(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, refs ...string) {
	t.Helper()
	for _, ref := range refs {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      ref,
			Title:       ref,
			Content:     "rhizome_service_evidence_v1\nkind: test\nref: " + ref,
			UpdatedBy:   actorID,
		}); err != nil {
			t.Fatalf("seed service RPC evidence doc %s: %v", ref, err)
		}
	}
}

func seedServiceVentureRPCBudgetSpendEntry(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string, run sqlite.ServiceRunRecord, entryID string, amountMicros int64) string {
	t.Helper()
	reservationID := entryID + "-reservation"
	if _, err := store.ReserveBudget(ctx, sqlite.BudgetReservationInput{
		ReservationID:  reservationID,
		IdempotencyKey: reservationID,
		AccountID:      run.BudgetAccountID,
		WorkspaceID:    workspaceID,
		AgentID:        actorID,
		TaskID:         "service-venture-rpc-test",
		RunID:          run.RunID,
		ProviderID:     "service-venture",
		Model:          "external-spend",
		AmountMicros:   amountMicros,
		Reason:         "service venture RPC test spend reservation",
	}); err != nil {
		t.Fatalf("reserve service RPC budget spend %s: %v", entryID, err)
	}
	if _, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        entryID,
		IdempotencyKey: entryID,
		AccountID:      run.BudgetAccountID,
		ReservationID:  reservationID,
		WorkspaceID:    workspaceID,
		AgentID:        actorID,
		TaskID:         "service-venture-rpc-test",
		RunID:          run.RunID,
		ProviderID:     "service-venture",
		Model:          "external-spend",
		AmountMicros:   amountMicros,
		Reason:         "service venture RPC test spend capture",
	}); err != nil {
		t.Fatalf("capture service RPC budget spend %s: %v", entryID, err)
	}
	return entryID
}

func serviceRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	return raw
}
