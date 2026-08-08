package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildRSPBeliefReportTracksClaimDrift(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-drift")

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-validator",
		ClaimType:   "FACT",
		Status:      "CONFIRMED",
		Subject:     "Verifier trace",
		Body:        "Verifier confirms the deployment guardrail.",
		Summary:     "Verifier confirms the guardrail.",
		Confidence:  0.95,
		SourceKind:  "workspace_doc",
		SourceID:    scenario.docKey,
		TaskID:      scenario.taskID,
	}); err != nil {
		t.Fatalf("record validator claim: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-fact",
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "Deployment guardrail",
		Body:        "Deploy only inside the approved maintenance window.",
		Summary:     "Deployment must stay inside the maintenance window.",
		Confidence:  0.88,
		SourceKind:  "workspace_doc",
		SourceID:    scenario.docKey,
		TaskID:      scenario.taskID,
		Evidence:    []string{"validated_by:claim-rsp-validator"},
	}); err != nil {
		t.Fatalf("record fact claim: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityBeliefLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief calibration report for storage test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, scenario.workspaceID)

	report, err := store.BuildRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build rsp belief report: %v", err)
	}
	if !report.CapabilityFlags.AnomalyShadow || !report.CapabilityFlags.StateShadow {
		t.Fatalf("expected belief report to expose shipped rsp capability flags, got %+v", report.CapabilityFlags)
	}
	if !report.CapabilityFlags.BeliefLive {
		t.Fatalf("expected belief report to expose belief-live capability, got %+v", report.CapabilityFlags)
	}
	if report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		report.Calibration.Status != rspCalibrationStatusProvisional ||
		report.Calibration.CalibrationVersion != "belief-read-model-v2" {
		t.Fatalf("expected belief report to expose versioned provisional calibration contract, got %+v", report.Calibration)
	}
	if !containsString(report.Calibration.Unsupported, "historical_priors") ||
		!containsString(report.Calibration.Unsupported, "global_root_cause_coverage") {
		t.Fatalf("expected belief report calibration contract to keep unsupported semantics explicit, got %+v", report.Calibration)
	}
	if containsString(report.Calibration.Unsupported, "root_cause_independence") {
		t.Fatalf("expected belief report calibration contract to stop advertising blanket root-cause independence absence, got %+v", report.Calibration)
	}
	if report.TimeAuthority.WorkspaceID != scenario.workspaceID || report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief report to expose workspace time authority, got %+v", report.TimeAuthority)
	}
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp belief report generated_at %q to mirror authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}
	item, ok := findRSPBeliefClaim(report.Items, "claim-rsp-fact")
	if !ok {
		t.Fatalf("expected fact belief item, got %+v", report.Items)
	}
	if item.TimeAuthority.WorkspaceID != scenario.workspaceID || item.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief item to expose workspace time authority, got %+v", item.TimeAuthority)
	}
	if item.BeliefDomain != "FACT" || item.SignalType != rspBeliefSignalType || !item.ShadowMode {
		t.Fatalf("unexpected rsp belief item %+v", item)
	}
	if item.VerifierFresh != true || item.DriftScore != 0 {
		t.Fatalf("expected fresh verifier-backed fact, got %+v", item)
	}
	if len(item.EvidenceRefs) == 0 {
		t.Fatalf("expected belief item to surface evidence refs, got %+v", item)
	}
	if item.Posterior <= 0.70 || item.SuggestedState == "DISPUTED" {
		t.Fatalf("expected supportive belief posterior, got %+v", item)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Rsp Drift Doc",
		Content:     "Maintenance window changed after the belief snapshot.",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("update workspace doc: %v", err)
	}

	drifted, err := store.GetRSPBeliefClaim(ctx, scenario.workspaceID, "claim-rsp-fact")
	if err != nil {
		t.Fatalf("get rsp belief claim: %v", err)
	}
	if drifted.DriftState != "STALE" || drifted.DriftScore <= 0 {
		t.Fatalf("expected stale drift state after doc update, got %+v", drifted)
	}
	if drifted.TimeAuthority.WorkspaceID != scenario.workspaceID || drifted.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief claim getter to expose workspace time authority, got %+v", drifted.TimeAuthority)
	}
}

func TestSnapshotRSPBeliefReportAppendsSyntheticEvent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-snapshot")
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityBeliefLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable belief snapshot for test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("enable belief live capability: %v", err)
	}

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Canary rollout",
		Body:        "Use a guarded canary rollout for this task.",
		Summary:     "Canary rollout accepted.",
		Confidence:  0.93,
		SourceKind:  "workspace_memory",
		SourceID:    "tests",
		TaskID:      scenario.taskID,
	}); err != nil {
		t.Fatalf("record decision claim: %v", err)
	}

	result, err := store.SnapshotRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err != nil {
		t.Fatalf("snapshot rsp belief report: %v", err)
	}
	if result.Event.EventType != "rsp.belief_snapshot" || result.Event.EntityType != "rsp_belief" {
		t.Fatalf("unexpected rsp belief snapshot event %+v", result.Event)
	}
	if !isSyntheticOperationalEvent(result.Event) {
		t.Fatalf("expected synthetic rsp belief snapshot %+v", result.Event)
	}
	if result.Report.SignalType != rspBeliefSignalType || result.Report.ShadowPhase != rspBeliefShadowPhase {
		t.Fatalf("unexpected rsp belief snapshot report %+v", result.Report)
	}
	if result.Report.Calibration.SchemaVersion != rspCalibrationSchemaVersion ||
		result.Report.Calibration.CalibrationVersion != "belief-read-model-v2" {
		t.Fatalf("expected snapshot report to carry versioned belief calibration contract, got %+v", result.Report.Calibration)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Event.PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal belief snapshot payload: %v", err)
	}
	calibration, ok := payload["calibration"].(map[string]any)
	if !ok {
		t.Fatalf("expected belief snapshot payload to carry calibration contract, got %+v", payload)
	}
	if calibration["schema_version"] != rspCalibrationSchemaVersion ||
		calibration["calibration_version"] != "belief-read-model-v2" ||
		calibration["status"] != rspCalibrationStatusProvisional {
		t.Fatalf("expected belief snapshot payload calibration contract, got %+v", calibration)
	}
	if result.Report.TimeAuthority.WorkspaceID != scenario.workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected rsp belief snapshot report to expose workspace time authority, got %+v", result.Report.TimeAuthority)
	}
	if result.Report.GeneratedAt != result.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected rsp belief snapshot report generated_at %q to mirror authority reference_at %q", result.Report.GeneratedAt, result.Report.TimeAuthority.ReferenceAt)
	}
}

func TestBuildMemoryKernelPacketIncludesRSPBeliefUpdates(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-packet")

	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-constraint",
		ClaimType:   "CONSTRAINT",
		Status:      "ACTIVE",
		Subject:     "Deploy window",
		Body:        "Do not deploy outside the maintenance window.",
		Summary:     "Stay inside the maintenance window.",
		Confidence:  0.88,
		SourceKind:  "workspace_memory",
		SourceID:    "tests",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-packet-decision",
		ClaimType:   "DECISION",
		Status:      "CONFIRMED",
		Subject:     "Rollout strategy",
		Body:        "Use guarded canary rollout.",
		Summary:     "Canary rollout accepted.",
		Confidence:  0.95,
		SourceKind:  "workspace_memory",
		SourceID:    "tests",
		TaskID:      scenario.taskID,
	})
	recordMemoryPacketClaim(t, ctx, store, KnowledgeClaimInput{
		WorkspaceID: scenario.workspaceID,
		ClaimID:     "claim-rsp-packet-blocker",
		ClaimType:   "BLOCKER",
		Status:      "ACTIVE",
		Subject:     "Approval missing",
		Body:        "Need operator approval before rollout.",
		Summary:     "Operator approval still missing.",
		Confidence:  0.82,
		SourceKind:  "workspace_memory",
		SourceID:    "tests",
		TaskID:      scenario.taskID,
		Evidence:    []string{"blocks:claim-rsp-packet-decision"},
	})

	packet, err := store.BuildMemoryKernelPacket(ctx, MemoryPacketFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err != nil {
		t.Fatalf("build memory kernel packet: %v", err)
	}
	if len(packet.Coordination.BeliefUpdates) < 3 {
		t.Fatalf("expected rsp belief updates in kernel packet, got %+v", packet.Coordination.BeliefUpdates)
	}
	if !hasRSPBeliefUpdate(packet.Coordination.BeliefUpdates, "claim-rsp-constraint", "FACT") {
		t.Fatalf("expected fact-like constraint belief update, got %+v", packet.Coordination.BeliefUpdates)
	}
	if !hasRSPBeliefUpdate(packet.Coordination.BeliefUpdates, "claim-rsp-packet-decision", "DECISION") {
		t.Fatalf("expected decision belief update, got %+v", packet.Coordination.BeliefUpdates)
	}
	if !hasRSPBeliefUpdate(packet.Coordination.BeliefUpdates, "claim-rsp-packet-blocker", "BLOCKER") {
		t.Fatalf("expected blocker belief update, got %+v", packet.Coordination.BeliefUpdates)
	}
}

func TestBuildRSPBeliefReportTracksSourceDiversityAndIndependenceDiscount(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-diversity")

	for _, doc := range []struct {
		key     string
		content string
	}{
		{key: "doc-rsp-same", content: "Shared evidence source for same-root belief checks."},
		{key: "doc-rsp-diverse-a", content: "Diverse evidence source A."},
		{key: "doc-rsp-diverse-b", content: "Diverse evidence source B."},
		{key: "doc-rsp-diverse-base", content: "Base claim source for diverse evidence checks."},
	} {
		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: scenario.workspaceID,
			DocKey:      doc.key,
			Title:       doc.key,
			Content:     doc.content,
			UpdatedBy:   "tests",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc.key, err)
		}
	}

	for _, claim := range []KnowledgeClaimInput{
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-same-validator-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Same-source validator A",
			Body:        "Same-source validator A confirms the guardrail.",
			Summary:     "Same-source validator A.",
			Confidence:  0.94,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-same",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-same-validator-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Same-source validator B",
			Body:        "Same-source validator B confirms the guardrail.",
			Summary:     "Same-source validator B.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-same",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-diverse-validator-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Diverse validator A",
			Body:        "Diverse validator A confirms the guardrail.",
			Summary:     "Diverse validator A.",
			Confidence:  0.94,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-diverse-a",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-diverse-validator-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Diverse validator B",
			Body:        "Diverse validator B confirms the guardrail.",
			Summary:     "Diverse validator B.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-diverse-b",
			TaskID:      scenario.taskID,
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record evidence claim %s: %v", claim.ClaimID, err)
		}
	}

	for _, claim := range []KnowledgeClaimInput{
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-same-source",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Same-source evidence claim",
			Body:        "Multiple confirmations all point back to one source.",
			Summary:     "Same-source evidence should discount independence.",
			Confidence:  0.82,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-same",
			TaskID:      scenario.taskID,
			Evidence: []string{
				"validated_by:claim-rsp-same-validator-a",
				"supports:claim-rsp-same-validator-b",
			},
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-diverse-source",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Diverse evidence claim",
			Body:        "Independent confirmations arrive from separate sources.",
			Summary:     "Diverse evidence should retain more independence.",
			Confidence:  0.82,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-diverse-base",
			TaskID:      scenario.taskID,
			Evidence: []string{
				"validated_by:claim-rsp-diverse-validator-a",
				"supports:claim-rsp-diverse-validator-b",
			},
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record target claim %s: %v", claim.ClaimID, err)
		}
	}

	report, err := store.BuildRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build rsp belief report: %v", err)
	}
	same, ok := findRSPBeliefClaim(report.Items, "claim-rsp-same-source")
	if !ok {
		t.Fatalf("expected same-source belief item, got %+v", report.Items)
	}
	diverse, ok := findRSPBeliefClaim(report.Items, "claim-rsp-diverse-source")
	if !ok {
		t.Fatalf("expected diverse-source belief item, got %+v", report.Items)
	}
	if same.EvidenceUnitCount != 3 || diverse.EvidenceUnitCount != 3 {
		t.Fatalf("expected bounded evidence unit counts, same=%+v diverse=%+v", same, diverse)
	}
	if same.SourceDiversity >= diverse.SourceDiversity {
		t.Fatalf("expected diverse-source claim to carry higher source diversity, same=%+v diverse=%+v", same, diverse)
	}
	if same.IndependenceDiscount >= diverse.IndependenceDiscount {
		t.Fatalf("expected diverse-source claim to keep a higher independence discount, same=%+v diverse=%+v", same, diverse)
	}
	if same.IndependentGroups >= diverse.IndependentGroups {
		t.Fatalf("expected same-source evidence to collapse into fewer independent groups, same=%+v diverse=%+v", same, diverse)
	}
	if same.CorrelatedEvidence == 0 {
		t.Fatalf("expected same-source evidence to surface correlated evidence count, got %+v", same)
	}
	if diverse.CorrelatedEvidence != 0 {
		t.Fatalf("expected diverse evidence not to collapse into correlated evidence, got %+v", diverse)
	}
	if same.Posterior >= diverse.Posterior {
		t.Fatalf("expected diverse evidence to preserve a stronger posterior after discounting, same=%+v diverse=%+v", same, diverse)
	}
	if same.Uncertainty <= diverse.Uncertainty {
		t.Fatalf("expected same-source evidence to remain more uncertain, same=%+v diverse=%+v", same, diverse)
	}
}

func TestBuildRSPBeliefReportCollapsesSharedUpstreamEvidence(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-shared-upstream")

	for _, doc := range []string{"doc-rsp-root", "doc-rsp-child-a", "doc-rsp-child-b", "doc-rsp-target"} {
		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: scenario.workspaceID,
			DocKey:      doc,
			Title:       doc,
			Content:     "belief shared-upstream fixture " + doc,
			UpdatedBy:   "tests",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc, err)
		}
	}

	for _, claim := range []KnowledgeClaimInput{
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-shared-upstream-root",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Shared upstream validator",
			Body:        "Shared upstream validator for downstream evidence claims.",
			Summary:     "Shared upstream validator.",
			Confidence:  0.96,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-root",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-child-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Child validator A",
			Body:        "Child validator A inherits the same upstream support.",
			Summary:     "Child validator A.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-child-a",
			TaskID:      scenario.taskID,
			Evidence:    []string{"validated_by:claim-rsp-shared-upstream-root"},
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-child-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Child validator B",
			Body:        "Child validator B inherits the same upstream support.",
			Summary:     "Child validator B.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-child-b",
			TaskID:      scenario.taskID,
			Evidence:    []string{"validated_by:claim-rsp-shared-upstream-root"},
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-shared-root-target",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Shared-root target",
			Body:        "Two child validators look separate by source but share the same upstream support.",
			Summary:     "Shared upstream evidence should collapse independence.",
			Confidence:  0.83,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-target",
			TaskID:      scenario.taskID,
			Evidence: []string{
				"supports:claim-rsp-child-a",
				"supports:claim-rsp-child-b",
			},
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record shared-upstream fixture claim %s: %v", claim.ClaimID, err)
		}
	}

	item, err := store.GetRSPBeliefClaim(ctx, scenario.workspaceID, "claim-rsp-shared-root-target")
	if err != nil {
		t.Fatalf("get rsp belief claim: %v", err)
	}
	if item.EvidenceUnitCount != 3 {
		t.Fatalf("expected bounded evidence unit count 3, got %+v", item)
	}
	if item.SourceDiversity <= item.EvidenceDiversity {
		t.Fatalf("expected shared-upstream evidence to retain broader source diversity than independence grouping, got %+v", item)
	}
	if item.IndependenceDiscount >= item.SourceDiversity {
		t.Fatalf("expected independence discount to remain below source diversity when root cause collapses, got %+v", item)
	}
	if item.IndependentGroups >= item.EvidenceUnitCount || item.CorrelatedEvidence == 0 {
		t.Fatalf("expected shared-root evidence to collapse correlated evidence groups, got %+v", item)
	}
}

func TestBuildRSPBeliefReportCollapsesSharedRuntimeRootCauseEvidence(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-runtime-root-cause")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range []RuntimeEventInput{
		{
			EventID:           "rtev-rsp-belief-root-a",
			WorkspaceID:       scenario.workspaceID,
			EventType:         "tests.rsp.belief_runtime_root_a",
			EntityType:        "runtime_event",
			EntityID:          "rtev-rsp-belief-root-a",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-belief-runtime-shared",
			ProvenanceGroupID: "PG-rsp-belief-runtime-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-belief-root-b",
			WorkspaceID:       scenario.workspaceID,
			EventType:         "tests.rsp.belief_runtime_root_b",
			EntityType:        "runtime_event",
			EntityID:          "rtev-rsp-belief-root-b",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-belief-runtime-shared",
			ProvenanceGroupID: "PG-rsp-belief-runtime-shared",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
		{
			EventID:           "rtev-rsp-belief-root-c",
			WorkspaceID:       scenario.workspaceID,
			EventType:         "tests.rsp.belief_runtime_root_c",
			EntityType:        "runtime_event",
			EntityID:          "rtev-rsp-belief-root-c",
			ActorType:         "tester",
			ActorID:           "tester",
			RootCauseID:       "RC-rsp-belief-runtime-distinct",
			ProvenanceGroupID: "PG-rsp-belief-runtime-distinct",
			PayloadJSON:       `{}`,
			CreatedAt:         now,
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record runtime root-cause fixture event %s: %v", event.EventID, err)
		}
	}

	for _, claim := range []KnowledgeClaimInput{
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-runtime-child-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Runtime child A",
			Body:        "First validator derives from runtime event A.",
			Summary:     "Runtime child A.",
			Confidence:  0.93,
			SourceKind:  "runtime_event",
			SourceID:    "rtev-rsp-belief-root-a",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-runtime-child-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Runtime child B",
			Body:        "Second validator derives from runtime event B.",
			Summary:     "Runtime child B.",
			Confidence:  0.92,
			SourceKind:  "runtime_event",
			SourceID:    "rtev-rsp-belief-root-b",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-runtime-child-c",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Runtime child C",
			Body:        "Distinct validator derives from runtime event C.",
			Summary:     "Runtime child C.",
			Confidence:  0.92,
			SourceKind:  "runtime_event",
			SourceID:    "rtev-rsp-belief-root-c",
			TaskID:      scenario.taskID,
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-runtime-shared-root-target",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Shared runtime-root target",
			Body:        "Two validator claims look distinct by source id but share one runtime root cause.",
			Summary:     "Shared runtime-root evidence should collapse independence.",
			Confidence:  0.83,
			SourceKind:  "workspace_doc",
			SourceID:    scenario.docKey,
			TaskID:      scenario.taskID,
			Evidence: []string{
				"supports:claim-rsp-runtime-child-a",
				"supports:claim-rsp-runtime-child-b",
			},
		},
		{
			WorkspaceID: scenario.workspaceID,
			ClaimID:     "claim-rsp-runtime-distinct-root-target",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Distinct runtime-root target",
			Body:        "Validators derive from different runtime root causes and should stay more independent.",
			Summary:     "Distinct runtime-root evidence should retain more independence.",
			Confidence:  0.83,
			SourceKind:  "workspace_doc",
			SourceID:    scenario.docKey,
			TaskID:      scenario.taskID,
			Evidence: []string{
				"supports:claim-rsp-runtime-child-a",
				"supports:claim-rsp-runtime-child-c",
			},
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record runtime root-cause fixture claim %s: %v", claim.ClaimID, err)
		}
	}

	shared, err := store.GetRSPBeliefClaim(ctx, scenario.workspaceID, "claim-rsp-runtime-shared-root-target")
	if err != nil {
		t.Fatalf("get shared-root target belief claim: %v", err)
	}
	distinct, err := store.GetRSPBeliefClaim(ctx, scenario.workspaceID, "claim-rsp-runtime-distinct-root-target")
	if err != nil {
		t.Fatalf("get distinct-root target belief claim: %v", err)
	}

	if shared.IndependenceBasis != "ROOT_CAUSE_MIXED" {
		t.Fatalf("expected shared-root target to expose mixed root-cause independence basis, got %+v", shared)
	}
	if len(shared.RootCauseGroups) != 1 || !containsLocusString(shared.RootCauseGroups, "RC-rsp-belief-runtime-shared") {
		t.Fatalf("expected shared-root target to collapse onto one root-cause group, got %+v", shared)
	}
	if shared.IndependentGroups >= distinct.IndependentGroups {
		t.Fatalf("expected shared-root target to collapse into fewer independent groups than distinct-root target, shared=%+v distinct=%+v", shared, distinct)
	}
	if shared.IndependenceDiscount >= distinct.IndependenceDiscount {
		t.Fatalf("expected shared-root target to keep a lower independence discount than distinct-root target, shared=%+v distinct=%+v", shared, distinct)
	}
	if distinct.IndependenceBasis != "ROOT_CAUSE_MIXED" {
		t.Fatalf("expected distinct-root target to remain root-cause aware even without collapse, got %+v", distinct)
	}
	if len(distinct.RootCauseGroups) != 2 {
		t.Fatalf("expected distinct-root target to retain two runtime root-cause groups, got %+v", distinct)
	}
}

func TestBuildRSPBeliefReportSurfacesAggregateDiagnostics(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-rsp-belief-diagnostics"
		taskID      = "task-rsp-belief-diagnostics"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-rsp-belief-diagnostics")
	for _, doc := range []string{"doc-rsp-belief-shared", "doc-rsp-belief-disputed"} {
		if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc,
			Title:       doc,
			Content:     "belief diagnostics fixture " + doc,
			UpdatedBy:   "tests",
		}); err != nil {
			t.Fatalf("upsert workspace doc %s: %v", doc, err)
		}
	}

	for _, claim := range []KnowledgeClaimInput{
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-rsp-belief-diagnostics-validator-a",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Shared validator A",
			Body:        "Shared validator A confirms the same-source belief claim.",
			Summary:     "Shared validator A.",
			Confidence:  0.94,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-belief-shared",
			TaskID:      taskID,
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-rsp-belief-diagnostics-validator-b",
			ClaimType:   "FACT",
			Status:      "CONFIRMED",
			Subject:     "Shared validator B",
			Body:        "Shared validator B confirms the same-source belief claim.",
			Summary:     "Shared validator B.",
			Confidence:  0.92,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-belief-shared",
			TaskID:      taskID,
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-rsp-belief-diagnostics-same-source",
			ClaimType:   "FACT",
			Status:      "ACTIVE",
			Subject:     "Same-source target",
			Body:        "Multiple confirmations all resolve to the same source.",
			Summary:     "Same-source target.",
			Confidence:  0.84,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-belief-shared",
			TaskID:      taskID,
			Evidence: []string{
				"validated_by:claim-rsp-belief-diagnostics-validator-a",
				"supports:claim-rsp-belief-diagnostics-validator-b",
			},
		},
		{
			WorkspaceID: workspaceID,
			ClaimID:     "claim-rsp-belief-diagnostics-disputed",
			ClaimType:   "FACT",
			Status:      "DISPUTED",
			Subject:     "Disputed target",
			Body:        "This belief item remains disputed and verifier-stale.",
			Summary:     "Disputed target.",
			Confidence:  0.41,
			SourceKind:  "workspace_doc",
			SourceID:    "doc-rsp-belief-disputed",
			TaskID:      taskID,
		},
	} {
		if _, err := store.RecordKnowledgeClaim(ctx, claim); err != nil {
			t.Fatalf("record diagnostics claim %s: %v", claim.ClaimID, err)
		}
	}

	report, err := store.BuildRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build rsp belief report: %v", err)
	}
	if report.Count != 4 {
		t.Fatalf("expected bounded diagnostics fixture to surface 4 claims, got %+v", report)
	}
	if report.LowIndependenceCount != 1 {
		t.Fatalf("expected one low-independence belief item, got %+v", report)
	}
	if report.HighContradictionCount != 1 {
		t.Fatalf("expected one high-contradiction belief item, got %+v", report)
	}
	if report.VerifierStaleCount != 1 {
		t.Fatalf("expected one verifier-stale belief item, got %+v", report)
	}
	if report.HighUncertaintyCount != 4 {
		t.Fatalf("expected all bounded diagnostics items to remain high-uncertainty, got %+v", report)
	}

	sameSource, ok := findRSPBeliefClaim(report.Items, "claim-rsp-belief-diagnostics-same-source")
	if !ok {
		t.Fatalf("expected same-source diagnostics claim, got %+v", report.Items)
	}
	if sameSource.IndependenceDiscount >= rspBeliefLowIndependenceCutoff {
		t.Fatalf("expected same-source claim to remain below low-independence cutoff, got %+v", sameSource)
	}

	disputed, ok := findRSPBeliefClaim(report.Items, "claim-rsp-belief-diagnostics-disputed")
	if !ok {
		t.Fatalf("expected disputed diagnostics claim, got %+v", report.Items)
	}
	if disputed.VerifierFresh || disputed.ContradictionPressure < rspBeliefHighContradictionCutoff || disputed.Uncertainty < rspBeliefHighUncertaintyCutoff {
		t.Fatalf("expected disputed diagnostics claim to drive contradiction/staleness/uncertainty counts, got %+v", disputed)
	}
}

func findRSPBeliefClaim(items []RSPBeliefClaimReport, claimID string) (RSPBeliefClaimReport, bool) {
	for _, item := range items {
		if item.ClaimID == claimID {
			return item, true
		}
	}
	return RSPBeliefClaimReport{}, false
}

func hasRSPBeliefUpdate(items []RSPBeliefClaimReport, claimID, domain string) bool {
	for _, item := range items {
		if item.ClaimID == claimID && item.BeliefDomain == domain {
			return true
		}
	}
	return false
}

func TestRSPBeliefSemanticTypeCompatibilityFilters(t *testing.T) {
	t.Parallel()

	decisionTypes := rspBeliefFilterClaimTypes("DECISION_RECORD")
	if len(decisionTypes) != 2 || decisionTypes[0] != "DECISION" || decisionTypes[1] != "DECISION_RECORD" {
		t.Fatalf("expected DECISION_RECORD filter to cover legacy and canonical decision types, got %+v", decisionTypes)
	}
	if got := rspBeliefDomainForClaimType("DECISION_RECORD"); got != "DECISION" {
		t.Fatalf("rspBeliefDomainForClaimType(DECISION_RECORD) = %q, want DECISION", got)
	}

	blockerTypes := rspBeliefFilterClaimTypes("BLOCKER_SYMPTOM")
	if len(blockerTypes) != 3 || blockerTypes[0] != "BLOCKER" || blockerTypes[1] != "INCIDENT" || blockerTypes[2] != "BLOCKER_SYMPTOM" {
		t.Fatalf("expected BLOCKER_SYMPTOM filter to cover legacy blocker aliases, got %+v", blockerTypes)
	}
	if got := rspBeliefDomainForClaimType("BLOCKER_SYMPTOM"); got != "BLOCKER" {
		t.Fatalf("rspBeliefDomainForClaimType(BLOCKER_SYMPTOM) = %q, want BLOCKER", got)
	}
	if got := rspBeliefDomainForClaimType("BLOCKER_HYPOTHESIS"); got != "BLOCKER" {
		t.Fatalf("rspBeliefDomainForClaimType(BLOCKER_HYPOTHESIS) = %q, want BLOCKER", got)
	}
}
