package server

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceTensionRPCSurfaceAndSSE(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "Instrumentation RPC runbook v2",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc for gap threshold: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:  "artifact-instrumentation-rpc-gap",
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.primaryTaskID,
		Title:       "Gap Evidence",
		ArtifactRef: "artifact://instrumentation-rpc-gap",
		Kind:        "note",
		ContentType: "text/plain",
		CreatedBy:   "agent-b",
	}); err != nil {
		t.Fatalf("create extra workspace artifact for gap threshold: %v", err)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	result, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}
	refreshPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected refresh result type %T", result)
	}
	refresh, ok := refreshPayload["refresh"].(sqlite.TensionRefreshResult)
	if !ok {
		t.Fatalf("unexpected refresh payload type %T", refreshPayload["refresh"])
	}
	if refresh.WorkspaceID != scenario.workspaceID {
		t.Fatalf("expected refresh workspace %s, got %+v", scenario.workspaceID, refresh)
	}
	if refresh.EvaluatedClusters == 0 || len(refresh.Report.Frontier) == 0 || len(refresh.Events) == 0 {
		t.Fatalf("expected non-empty refresh result, got %+v", refresh)
	}
	if refresh.Report.FrontierCapacity <= 0 || refresh.Report.FreeAgentCount < 0 {
		t.Fatalf("expected refresh report to expose frontier capacity and free-agent count, got %+v", refresh.Report)
	}
	refreshAuthority, ok := refreshPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || refreshAuthority.WorkspaceID != scenario.workspaceID || refreshAuthority.ReferenceAt == "" {
		t.Fatalf("expected refresh time authority, got %+v", refreshPayload["time_authority"])
	}
	if refresh.TimeAuthority.WorkspaceID != scenario.workspaceID || refresh.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected embedded refresh time authority, got %+v", refresh.TimeAuthority)
	}
	if refresh.RefreshedAt != refresh.TimeAuthority.ReferenceAt {
		t.Fatalf("expected embedded refresh refreshed_at %q to mirror authority reference_at %q", refresh.RefreshedAt, refresh.TimeAuthority.ReferenceAt)
	}
	if refresh.Report.GeneratedAt != refresh.Report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected embedded refresh report generated_at %q to mirror authority reference_at %q", refresh.Report.GeneratedAt, refresh.Report.TimeAuthority.ReferenceAt)
	}
	if refresh.Report.TimeAuthority.WorkspaceID != scenario.workspaceID || refresh.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", refresh.Report.TimeAuthority)
	}
	for _, persisted := range refresh.Events {
		live := nextEvent(t, ch)
		assertLiveEventMirrorsRuntimeEvent(t, live, persisted, "")
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), persisted.PayloadJSON)
		wantTensionID := persisted.EntityID
		if persisted.EventType == "tension.refreshed" {
			wantTensionID = ""
		}
		assertServerWorkspaceTensionRuntimePromptContext(t, persisted, "workspace.tension.refresh", scenario.workspaceID, wantTensionID, "human", "dashboard", map[string]string{
			"event_kind": persisted.EventType,
			"actor_type": "system",
			"actor_id":   "dashboard",
		})
	}

	rawList, err := json.Marshal(workspaceTensionListParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr = h.workspaceTensionList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionList rpc error: %+v", rpcErr)
	}
	listPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", result)
	}
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != scenario.workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	listItems, ok := listPayload["items"].([]sqlite.TensionRecord)
	if !ok || len(listItems) == 0 {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}
	primary := requireTensionRecordByType(t, listItems, "bottleneck")
	if primary.Kind != "atomic" || primary.SurfacedPriority <= 0 {
		t.Fatalf("expected list surface to expose structural tension fields, got %+v", primary)
	}
	if primary.RecoveryRisk <= 0 {
		t.Fatalf("expected list surface to expose advisory recovery risk, got %+v", primary)
	}
	dismissible := requireTensionRecordByType(t, listItems, "gap")

	rawGet, err := json.Marshal(workspaceTensionGetParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	result, rpcErr = h.workspaceTensionGet(ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionGet rpc error: %+v", rpcErr)
	}
	detail, ok := result.(sqlite.TensionDetail)
	if !ok {
		t.Fatalf("unexpected get detail type %T", result)
	}
	if detail.Tension.TensionID != primary.TensionID {
		t.Fatalf("expected tension %s, got %+v", primary.TensionID, detail.Tension)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected detail time authority, got %+v", detail.TimeAuthority)
	}
	if len(detail.Evidence) == 0 || detail.ProtoCluster == nil {
		t.Fatalf("expected populated tension detail, got %+v", detail)
	}
	if len(detail.Tension.SegmentRefs) == 0 {
		t.Fatalf("expected tension detail to surface segment_refs for UI linking, got %+v", detail.Tension)
	}
	if len(detail.Tension.ConstraintRefs) == 0 {
		t.Fatalf("expected tension detail to surface constraint_refs for UI linking, got %+v", detail.Tension)
	}
	if detail.Tension.Kind != "atomic" || detail.Tension.BaseImportance <= 0 || detail.Tension.VisibilityScore <= 0 || detail.Tension.SurfacedPriority <= 0 {
		t.Fatalf("expected tension detail to expose structural scoring fields, got %+v", detail.Tension)
	}
	if detail.Tension.RecoveryRisk <= 0 || detail.Tension.ArchivePropensity < 0 {
		t.Fatalf("expected tension detail to expose advisory archive/recovery fields, got %+v", detail.Tension)
	}

	rawFrontier, err := json.Marshal(workspaceTensionFrontierParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("marshal frontier params: %v", err)
	}
	result, rpcErr = h.workspaceTensionFrontier(ctx, rawFrontier)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionFrontier rpc error: %+v", rpcErr)
	}
	frontierPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected frontier result type %T", result)
	}
	frontierAuthority, ok := frontierPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || frontierAuthority.WorkspaceID != scenario.workspaceID || frontierAuthority.ReferenceAt == "" {
		t.Fatalf("expected frontier time authority, got %+v", frontierPayload["time_authority"])
	}
	frontierItems, ok := frontierPayload["items"].([]sqlite.TensionFrontierItem)
	if !ok || len(frontierItems) == 0 {
		t.Fatalf("unexpected frontier payload %+v", frontierPayload)
	}
	if frontierItems[0].Kind == "" || frontierItems[0].SurfacedPriority <= 0 {
		t.Fatalf("expected frontier payload to expose structural fields, got %+v", frontierItems[0])
	}
	if frontierItems[0].RecoveryRisk <= 0 {
		t.Fatalf("expected frontier payload to expose advisory recovery risk, got %+v", frontierItems[0])
	}

	confirm := requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionConfirm, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "operator confirmed",
	}, "tension.confirmed")
	if confirm.Tension.ReviewStatus != "CONFIRMED" {
		t.Fatalf("expected confirmed tension, got %+v", confirm.Tension)
	}

	discard := requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionDiscard, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   dismissible.TensionID,
		ActorID:     "dashboard",
		Reason:      "duplicate hotspot",
	}, "tension.discarded")
	if discard.Tension.ReviewStatus != "DISCARDED" {
		t.Fatalf("expected discarded tension, got %+v", discard.Tension)
	}

	_ = requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionResolve, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "resolve before archive",
	}, "tension.resolved")

	archive := requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionArchive, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "park after confirmation",
	}, "tension.archived")
	if archive.Tension.LifecycleState != "ARCHIVED" {
		t.Fatalf("expected archived tension, got %+v", archive.Tension)
	}

	rawArchived, err := json.Marshal(workspaceTensionListParams{
		WorkspaceID:    scenario.workspaceID,
		LifecycleState: "ARCHIVED",
		Limit:          20,
	})
	if err != nil {
		t.Fatalf("marshal archived list params: %v", err)
	}
	result, rpcErr = h.workspaceTensionList(ctx, rawArchived)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionList archived rpc error: %+v", rpcErr)
	}
	archivedPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected archived list result type %T", result)
	}
	archivedItems, ok := archivedPayload["items"].([]sqlite.TensionRecord)
	if !ok || len(archivedItems) == 0 {
		t.Fatalf("unexpected archived payload %+v", archivedPayload)
	}

}

func TestWorkspaceTensionAttachableSurfaceExposesAttachmentFactors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh); rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}

	rawAttachable, err := json.Marshal(workspaceTensionAttachableParams{
		WorkspaceID: scenario.workspaceID,
		AgentID:     "agent-a",
	})
	if err != nil {
		t.Fatalf("marshal attachable params: %v", err)
	}
	result, rpcErr := h.workspaceTensionListAttachable(ctx, rawAttachable)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionListAttachable rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected attachable result type %T", result)
	}
	items, ok := payload["items"].([]sqlite.ScoredTension)
	if !ok || len(items) == 0 {
		t.Fatalf("unexpected attachable payload %+v", payload)
	}
	if items[0].AttachProb <= 0 || items[0].AttachScore == 0 {
		t.Fatalf("expected attach probability and score, got %+v", items[0])
	}
	if items[0].AttachFactors.Fit <= 0 || items[0].AttachFactors.CrowdingRatio < 0 {
		t.Fatalf("expected attachment factors to be surfaced, got %+v", items[0].AttachFactors)
	}
	if items[0].AttachFactors.FarReviewerRelief < 0 {
		t.Fatalf("expected far-reviewer relief to stay bounded on surfaced attachment factors, got %+v", items[0].AttachFactors)
	}
	advisorySeen := false
	for _, item := range items {
		if item.AttachFactors.RecoveryRisk > 0 || item.AttachFactors.ArchivePropensity > 0 || item.AttachFactors.LeaseSensitive {
			advisorySeen = true
			break
		}
	}
	if !advisorySeen {
		t.Fatalf("expected attachable payload to surface advisory frontier pressure factors, got %+v", items)
	}
}

func TestWorkspaceTensionAttachAgentPersistsShortlistHeuristicFactors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh); rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}

	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID:    scenario.workspaceID,
		LifecycleState: "ACTIVE",
		Limit:          20,
	})
	if err != nil {
		t.Fatalf("list tensions after refresh: %v", err)
	}
	target := requireTensionRecordByType(t, items, "bottleneck")

	expectedFactors := make(map[string]sqlite.AgentAttachmentFactors, 2)
	for _, agentID := range []string{"agent-b", "agent-a"} {
		scored, err := store.ListAgentAvailableTensionsScored(ctx, scenario.workspaceID, agentID)
		if err != nil {
			t.Fatalf("list scored tensions for %s: %v", agentID, err)
		}
		found := false
		for _, item := range scored {
			if item.TensionID == target.TensionID {
				expectedFactors[agentID] = item.AttachFactors
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected target tension %s to remain attachable for %s in %+v", target.TensionID, agentID, scored)
		}

		rawAttach, err := json.Marshal(workspaceTensionAgentActionParams{
			WorkspaceID:      scenario.workspaceID,
			TensionID:        target.TensionID,
			AgentID:          agentID,
			ActorID:          "dashboard",
			SuccessCriterion: "persist shortlist heuristic factors",
		})
		if err != nil {
			t.Fatalf("marshal attach params for %s: %v", agentID, err)
		}
		if _, rpcErr := h.workspaceTensionAttachAgent(ctx, rawAttach); rpcErr != nil {
			t.Fatalf("workspaceTensionAttachAgent rpc error for %s: %+v", agentID, rpcErr)
		}
	}

	coalition, err := store.GetTensionCoalition(ctx, scenario.workspaceID, target.TensionID)
	if err != nil {
		t.Fatalf("get coalition after attach: %v", err)
	}
	if coalition == nil || len(coalition.Members) != 2 {
		t.Fatalf("expected two-member coalition after attaches, got %+v", coalition)
	}

	rolesByAgent := make(map[string]string, len(coalition.Members))
	fitByAgent := make(map[string]float64, len(coalition.Members))
	for _, member := range coalition.Members {
		want, ok := expectedFactors[member.AgentID]
		if !ok {
			t.Fatalf("missing expected attach factors for %+v", member)
		}
		if math.Abs(member.FitScore-want.Fit) > 1e-9 || math.Abs(member.NoveltyScore-want.Novelty) > 1e-9 {
			t.Fatalf("expected persisted coalition member to mirror shortlist heuristic factors, got member=%+v want=%+v", member, want)
		}
		rolesByAgent[member.AgentID] = member.Role
		fitByAgent[member.AgentID] = member.FitScore
	}
	generatorID := ""
	for agentID, role := range rolesByAgent {
		if role == "GENERATOR" {
			generatorID = agentID
			break
		}
	}
	if generatorID == "" {
		t.Fatalf("expected one normalized generator after attach, got roles %+v", rolesByAgent)
	}
	for agentID, fit := range fitByAgent {
		if agentID == generatorID {
			continue
		}
		if fitByAgent[generatorID] < fit {
			t.Fatalf("expected generator to preserve the strongest persisted fit score, got roles=%+v fits=%+v", rolesByAgent, fitByAgent)
		}
	}
}

func TestWorkspaceTensionDependencyMutationCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	primary := seedServerTensionAuthoritySurface(t, ctx, store, scenario)
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list seeded tensions: %v", err)
	}
	child := requireTensionRecordByType(t, items, "gap")

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	addRaw, err := json.Marshal(workspaceTensionDependencyParams{
		WorkspaceID:        scenario.workspaceID,
		TensionID:          child.TensionID,
		DependsOnTensionID: primary.TensionID,
		DependencyType:     "SUBSUMED_BY",
		ActorID:            "dashboard",
		Reason:             "server dependency add",
	})
	if err != nil {
		t.Fatalf("marshal add dependency params: %v", err)
	}
	addResultRaw, rpcErr := h.workspaceTensionAddDependency(ctx, addRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionAddDependency rpc error: %+v", rpcErr)
	}
	addResult, ok := addResultRaw.(map[string]any)
	if !ok {
		t.Fatalf("unexpected add dependency result %T", addResultRaw)
	}
	addEvent, ok := addResult["event"].(sqlite.RuntimeEventRecord)
	if !ok || addEvent.EventType != "tension.dependency.added" {
		t.Fatalf("expected tension.dependency.added event, got %+v", addResult["event"])
	}
	addLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, addLive, addEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, addLive.PayloadJSON), addEvent.PayloadJSON)
	assertServerWorkspaceTensionRuntimePromptContext(t, addEvent, "workspace.tension.add.dependency", scenario.workspaceID, child.TensionID, "human", "dashboard", map[string]string{
		"event_kind":            "tension.dependency.added",
		"actor_type":            "operator",
		"actor_id":              "dashboard",
		"depends_on_tension_id": primary.TensionID,
		"dependency_type":       "SUBSUMED_BY",
	})
	if got := countServerTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, primary.TensionID); got != 1 {
		t.Fatalf("expected dependency edge after add, got %d", got)
	}

	removeRaw, err := json.Marshal(workspaceTensionDependencyParams{
		WorkspaceID:        scenario.workspaceID,
		TensionID:          child.TensionID,
		DependsOnTensionID: primary.TensionID,
		ActorID:            "dashboard",
		Reason:             "server dependency remove",
	})
	if err != nil {
		t.Fatalf("marshal remove dependency params: %v", err)
	}
	removeResultRaw, rpcErr := h.workspaceTensionRemoveDependency(ctx, removeRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionRemoveDependency rpc error: %+v", rpcErr)
	}
	removeResult, ok := removeResultRaw.(map[string]any)
	if !ok {
		t.Fatalf("unexpected remove dependency result %T", removeResultRaw)
	}
	removeEvent, ok := removeResult["event"].(sqlite.RuntimeEventRecord)
	if !ok || removeEvent.EventType != "tension.dependency.removed" {
		t.Fatalf("expected tension.dependency.removed event, got %+v", removeResult["event"])
	}
	removeLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, removeLive, removeEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, removeLive.PayloadJSON), removeEvent.PayloadJSON)
	assertServerWorkspaceTensionRuntimePromptContext(t, removeEvent, "workspace.tension.remove.dependency", scenario.workspaceID, child.TensionID, "human", "dashboard", map[string]string{
		"event_kind":            "tension.dependency.removed",
		"actor_type":            "operator",
		"actor_id":              "dashboard",
		"depends_on_tension_id": primary.TensionID,
		"dependency_type":       "SUBSUMED_BY",
	})
	if got := countServerTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, primary.TensionID); got != 0 {
		t.Fatalf("expected dependency edge removed, got %d", got)
	}
}

func TestWorkspaceTensionRemoveDependencyRejectsInvalidTargetWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	_ = seedServerTensionAuthoritySurface(t, ctx, store, scenario)
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list seeded tensions: %v", err)
	}
	child := requireTensionRecordByType(t, items, "gap")
	const otherWorkspaceID = "ws-server-tension-dependency-invalid-target-other"
	const otherTensionID = "tension-server-dependency-other-workspace"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: otherWorkspaceID,
		Title:       "Other server dependency workspace",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
			title, summary, anchor_kind, anchor_ref, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		otherTensionID, otherWorkspaceID, "cluster-other", "gap", "ACTIVE", "PENDING",
		"Other workspace tension", "Cross-workspace dependency target", "test", "other",
	); err != nil {
		t.Fatalf("insert other workspace tension: %v", err)
	}
	beforeRemoved := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.dependency.removed", child.TensionID)

	raw, err := json.Marshal(workspaceTensionDependencyParams{
		WorkspaceID:        scenario.workspaceID,
		TensionID:          child.TensionID,
		DependsOnTensionID: otherTensionID,
		ActorID:            "dashboard",
		Reason:             "invalid remove target",
	})
	if err != nil {
		t.Fatalf("marshal invalid remove params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionRemoveDependency(ctx, raw); rpcErr == nil {
		t.Fatal("expected invalid remove target to fail")
	}
	if got := countServerTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, otherTensionID); got != 0 {
		t.Fatalf("expected no dependency edge after invalid remove, got %d", got)
	}
	if got := countServerTensionRuntimeEvents(t, ctx, store, scenario.workspaceID, "tension.dependency.removed", child.TensionID); got != beforeRemoved {
		t.Fatalf("expected no dependency removed event after invalid remove, before=%d after=%d", beforeRemoved, got)
	}
}

func TestWorkspaceTensionCondenseCarriesPromptContextEnvelopeAndLiveEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	seedServerMetaTensionCycle(t, ctx, store, scenario.workspaceID)

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	resultAny, rpcErr := h.workspaceTensionCondense(ctx, mustJSONRaw(workspaceTensionCondenseParams{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
		Reason:      "server condense cycle",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceTensionCondense rpc error: %+v", rpcErr)
	}
	payload, ok := resultAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected condense result type %T", resultAny)
	}
	result, ok := payload["result"].(sqlite.TensionCondenseResult)
	if !ok {
		t.Fatalf("unexpected condense payload type %T", payload["result"])
	}
	if result.CreatedCount != 1 || result.DependencyAddedCount != 3 || result.ProcessedComponentCount != 1 {
		t.Fatalf("unexpected condense result %+v", result)
	}
	if len(result.MetaTensionIDs) != 1 {
		t.Fatalf("expected one meta tension id, got %+v", result.MetaTensionIDs)
	}
	metaID := result.MetaTensionIDs[0]
	counts := map[string]int{}
	for _, event := range result.Events {
		counts[event.EventType]++
		live := nextEvent(t, ch)
		assertLiveEventMirrorsRuntimeEvent(t, live, event, "")
		assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), event.PayloadJSON)
		wantTensionID := event.EntityID
		if event.EventType == "tension.condensed" {
			wantTensionID = ""
		}
		extra := map[string]string{
			"event_kind": event.EventType,
			"actor_type": "system",
			"actor_id":   "dashboard",
		}
		if event.EventType == "tension.emergent" {
			extra["tension_type"] = "meta-tension"
			extra["anchor_kind"] = "scc_condensation"
			extra["scc_member_count"] = "3"
			extra["condense_action"] = "created"
			extra["scc_member_tension_ids"] = "srv-meta-t1,srv-meta-t2,srv-meta-t3"
		}
		if event.EventType == "tension.dependency.added" {
			extra["depends_on_tension_id"] = metaID
			extra["dependency_type"] = "SUBSUMED_BY"
			extra["scc_member_count"] = "3"
			extra["condense_action"] = "dependency_linked"
			extra["scc_member_tension_ids"] = "srv-meta-t1,srv-meta-t2,srv-meta-t3"
		}
		if event.EventType == "tension.condensed" {
			extra["changed"] = "true"
			extra["processed_component_count"] = "1"
			extra["dependency_added_count"] = "3"
		}
		assertServerWorkspaceTensionRuntimePromptContext(t, event, "workspace.tension.condense", scenario.workspaceID, wantTensionID, "human", "dashboard", extra)
	}
	for eventType, want := range map[string]int{
		"tension.emergent":         1,
		"tension.dependency.added": 3,
		"tension.condensed":        1,
	} {
		if counts[eventType] != want {
			t.Fatalf("expected %d %s events, got %+v", want, eventType, counts)
		}
	}
	if got := countServerMetaTensionRows(t, ctx, store, scenario.workspaceID); got != 1 {
		t.Fatalf("expected one meta tension row, got %d", got)
	}
}

func TestWorkspaceTensionCondenseRejectsMissingPrincipalWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	claimServerTestWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	seedServerMetaTensionCycle(t, ctx, store, scenario.workspaceID)

	beforeMeta := countServerMetaTensionRows(t, ctx, store, scenario.workspaceID)
	if _, rpcErr := h.workspaceTensionCondense(context.Background(), mustJSONRaw(workspaceTensionCondenseParams{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
		Reason:      "missing principal should fail",
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for condense without actor principal, got %+v", rpcErr)
	}
	if got := countServerMetaTensionRows(t, ctx, store, scenario.workspaceID); got != beforeMeta {
		t.Fatalf("expected no meta tension side effects after missing principal, before=%d after=%d", beforeMeta, got)
	}
	if got := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.condensed"); got != 0 {
		t.Fatalf("expected no condense summary event after missing principal, got %d", got)
	}
}

func TestWorkspaceTensionAgentAttachDetachCarriesPromptContextEnvelopeAndLiveEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	primary := seedServerTensionAuthoritySurface(t, ctx, store, scenario)
	const agentID = "agent-a"

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	attachResultAny, rpcErr := h.workspaceTensionAttachAgent(ctx, mustJSONRaw(workspaceTensionAgentActionParams{
		WorkspaceID:      scenario.workspaceID,
		TensionID:        primary.TensionID,
		AgentID:          agentID,
		ActorID:          "dashboard",
		SuccessCriterion: "server attach coalition prompt context",
		Reason:           "server attach coalition member",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceTensionAttachAgent rpc error: %+v", rpcErr)
	}
	attachResult, ok := attachResultAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected attach result type %T", attachResultAny)
	}
	if changed, _ := attachResult["changed"].(bool); !changed {
		t.Fatalf("expected attach to change coalition membership, got %+v", attachResult)
	}
	attachEvent, ok := attachResult["event"].(sqlite.RuntimeEventRecord)
	if !ok || attachEvent.EventType != "tension.agent.attached" {
		t.Fatalf("expected tension.agent.attached event, got %+v", attachResult["event"])
	}
	coalitionID, _ := attachResult["coalition_id"].(string)
	if coalitionID == "" {
		t.Fatalf("expected attach coalition id, got %+v", attachResult)
	}
	attachLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, attachLive, attachEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, attachLive.PayloadJSON), attachEvent.PayloadJSON)
	assertServerWorkspaceTensionRuntimePromptContext(t, attachEvent, "workspace.tension.agent.attach", scenario.workspaceID, primary.TensionID, "human", "dashboard", map[string]string{
		"event_kind":             "tension.agent.attached",
		"actor_type":             "operator",
		"actor_id":               "dashboard",
		"coalition_id":           coalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "attached",
		"coalition_member_count": "1",
		"coalition_status":       "FORMING",
	})
	if got := countServerTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, coalitionID, agentID); got != 1 {
		t.Fatalf("expected attached coalition member, got %d", got)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE workspace_coalition_members SET min_stay_until_epoch = 0 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, scenario.workspaceID, coalitionID, agentID); err != nil {
		t.Fatalf("relax coalition minimum tenure: %v", err)
	}
	detachResultAny, rpcErr := h.workspaceTensionDetachAgent(ctx, mustJSONRaw(workspaceTensionDetachParams{
		WorkspaceID: scenario.workspaceID,
		CoalitionID: coalitionID,
		AgentID:     agentID,
		ActorID:     "dashboard",
		Reason:      "server detach coalition member",
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceTensionDetachAgent rpc error: %+v", rpcErr)
	}
	detachResult, ok := detachResultAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected detach result type %T", detachResultAny)
	}
	detachEvent, ok := detachResult["event"].(sqlite.RuntimeEventRecord)
	if !ok || detachEvent.EventType != "tension.agent.detached" {
		t.Fatalf("expected tension.agent.detached event, got %+v", detachResult["event"])
	}
	detachLive := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, detachLive, detachEvent, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, detachLive.PayloadJSON), detachEvent.PayloadJSON)
	assertServerWorkspaceTensionRuntimePromptContext(t, detachEvent, "workspace.tension.agent.detach", scenario.workspaceID, primary.TensionID, "human", "dashboard", map[string]string{
		"event_kind":             "tension.agent.detached",
		"actor_type":             "operator",
		"actor_id":               "dashboard",
		"coalition_id":           coalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "detached",
		"coalition_member_count": "0",
		"coalition_status":       "DISBANDED",
	})
	if got := countServerTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, coalitionID, agentID); got != 0 {
		t.Fatalf("expected detached coalition member to be removed, got %d", got)
	}
}

func TestWorkspaceTensionAgentAttachRejectsMissingPrincipalWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	primary := seedServerTensionAuthoritySurface(t, ctx, store, scenario)
	beforeCoalitions := countServerTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID)

	if _, rpcErr := h.workspaceTensionAttachAgent(context.Background(), mustJSONRaw(workspaceTensionAgentActionParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		AgentID:     "agent-missing-principal-attach",
		ActorID:     "dashboard",
		Reason:      "missing principal should fail",
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for attach without actor principal, got %+v", rpcErr)
	}
	if got := countServerTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID); got != beforeCoalitions {
		t.Fatalf("expected no coalition side effect after missing principal, before=%d after=%d", beforeCoalitions, got)
	}
	if got := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.agent.attached"); got != 0 {
		t.Fatalf("expected no attach event after missing principal, got %d", got)
	}
}

func TestWorkspaceTensionRPCSurfaceExposesWorkspaceTimeAuthority(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	result, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}
	refreshPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected refresh result type %T", result)
	}
	refresh, ok := refreshPayload["refresh"].(sqlite.TensionRefreshResult)
	if !ok {
		t.Fatalf("unexpected refresh payload type %T", refreshPayload["refresh"])
	}
	refreshAuthority, ok := refreshPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || refreshAuthority.WorkspaceID != scenario.workspaceID || refreshAuthority.ReferenceAt == "" {
		t.Fatalf("expected refresh time authority, got %+v", refreshPayload["time_authority"])
	}
	if refresh.TimeAuthority.WorkspaceID != scenario.workspaceID || refresh.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected embedded refresh time authority, got %+v", refresh.TimeAuthority)
	}
	if refresh.RefreshedAt != refresh.TimeAuthority.ReferenceAt {
		t.Fatalf("expected embedded refresh refreshed_at %q to mirror authority reference_at %q", refresh.RefreshedAt, refresh.TimeAuthority.ReferenceAt)
	}

	rawList, err := json.Marshal(workspaceTensionListParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr = h.workspaceTensionList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionList rpc error: %+v", rpcErr)
	}
	listPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected list result type %T", result)
	}
	listAuthority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || listAuthority.WorkspaceID != scenario.workspaceID || listAuthority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	items, ok := listPayload["items"].([]sqlite.TensionRecord)
	if !ok || len(items) == 0 {
		t.Fatalf("unexpected list payload %+v", listPayload)
	}

	primary := requireTensionRecordByType(t, items, "bottleneck")
	rawGet, err := json.Marshal(workspaceTensionGetParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	result, rpcErr = h.workspaceTensionGet(ctx, rawGet)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionGet rpc error: %+v", rpcErr)
	}
	detail, ok := result.(sqlite.TensionDetail)
	if !ok {
		t.Fatalf("unexpected get detail type %T", result)
	}
	if detail.TimeAuthority.WorkspaceID != scenario.workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected detail time authority, got %+v", detail.TimeAuthority)
	}

	rawFrontier, err := json.Marshal(workspaceTensionFrontierParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("marshal frontier params: %v", err)
	}
	result, rpcErr = h.workspaceTensionFrontier(ctx, rawFrontier)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionFrontier rpc error: %+v", rpcErr)
	}
	frontierPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected frontier result type %T", result)
	}
	frontierAuthority, ok := frontierPayload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || frontierAuthority.WorkspaceID != scenario.workspaceID || frontierAuthority.ReferenceAt == "" {
		t.Fatalf("expected frontier time authority, got %+v", frontierPayload["time_authority"])
	}
}

func TestWorkspaceTensionRPCContractsRejectMissingRequiredParams(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	tests := []struct {
		name   string
		call   func(context.Context, json.RawMessage) (any, *RPCError)
		params any
	}{
		{
			name: "refresh",
			call: h.workspaceTensionRefresh,
			params: workspaceTensionRefreshParams{
				ActorID: "dashboard",
			},
		},
		{
			name: "list",
			call: h.workspaceTensionList,
			params: workspaceTensionListParams{
				Limit: 10,
			},
		},
		{
			name: "get",
			call: h.workspaceTensionGet,
			params: workspaceTensionGetParams{
				WorkspaceID: "ws-test",
			},
		},
		{
			name: "frontier",
			call: h.workspaceTensionFrontier,
			params: workspaceTensionFrontierParams{
				Limit: 5,
			},
		},
		{
			name: "confirm",
			call: h.workspaceTensionConfirm,
			params: workspaceTensionLifecycleParams{
				WorkspaceID: "ws-test",
				TensionID:   "tension-1",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatalf("marshal params: %v", err)
			}
			if _, rpcErr := tc.call(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
				t.Fatalf("expected invalid params error, got %+v", rpcErr)
			}
		})
	}
}

func TestWorkspaceTensionRefreshRejectsMissingPrincipalWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	scenario := seedInstrumentationRPCScenario(t, ctx, store)
	beforeDetected := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.detected")

	result, rpcErr := h.workspaceTensionRefresh(ctx, mustJSONRaw(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	}))
	if rpcErr == nil {
		t.Fatal("expected missing principal to reject tension refresh")
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied for missing principal, got %+v", rpcErr)
	}
	if result != nil {
		t.Fatalf("expected no result after missing principal reject, got %+v", result)
	}
	if got := countServerTensionRuntimeEventsByType(t, ctx, store, scenario.workspaceID, "tension.detected"); got != beforeDetected {
		t.Fatalf("expected missing-principal refresh not to append tension events, before=%d after=%d", beforeDetected, got)
	}
}

func TestWorkspaceTensionLifecycleUpdateCarriesPromptContextEnvelope(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	refreshRaw, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	refreshAny, rpcErr := h.workspaceTensionRefresh(ctx, refreshRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}
	refresh := refreshAny.(map[string]any)["refresh"].(sqlite.TensionRefreshResult)
	for range refresh.Events {
		_ = nextEvent(t, ch)
	}

	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after refresh: %v", err)
	}
	primary := requireTensionRecordByType(t, items, "bottleneck")
	_ = requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionConfirm, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "activate before lifecycle update",
	}, "tension.confirmed")

	updateRaw, err := json.Marshal(workspaceTensionLifecycleUpdateParams{
		WorkspaceID:    scenario.workspaceID,
		TensionID:      primary.TensionID,
		LifecycleState: "RESOLVED",
		UpdatedBy:      "dashboard",
		Reason:         "resolve through lifecycle update facade",
	})
	if err != nil {
		t.Fatalf("marshal lifecycle update params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionLifecycleUpdate(ctx, updateRaw); rpcErr != nil {
		t.Fatalf("workspaceTensionLifecycleUpdate rpc error: %+v", rpcErr)
	}
	live := nextEvent(t, ch)
	event := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.resolved",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       1,
	})
	assertLiveEventMirrorsRuntimeEvent(t, live, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), event.PayloadJSON)
	assertServerWorkspaceTensionRuntimePromptContext(t, event, "workspace.tension.lifecycle.update", scenario.workspaceID, primary.TensionID, "human", "dashboard", map[string]string{
		"event_kind": "tension.resolved",
		"actor_type": "operator",
		"actor_id":   "dashboard",
	})
}

func TestWorkspaceTensionFrontierSupportsReviewStatusFilter(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh); rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}

	rawList, err := json.Marshal(workspaceTensionListParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceTensionList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionList rpc error: %+v", rpcErr)
	}
	listPayload := result.(map[string]any)
	items := listPayload["items"].([]sqlite.TensionRecord)
	primary := requireTensionRecordByType(t, items, "bottleneck")

	rawConfirm, err := json.Marshal(workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "confirm for frontier filter",
	})
	if err != nil {
		t.Fatalf("marshal confirm params: %v", err)
	}
	if _, rpcErr := h.workspaceTensionConfirm(ctx, rawConfirm); rpcErr != nil {
		t.Fatalf("workspaceTensionConfirm rpc error: %+v", rpcErr)
	}

	rawFrontier, err := json.Marshal(workspaceTensionFrontierParams{
		WorkspaceID:  scenario.workspaceID,
		ReviewStatus: "CONFIRMED",
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("marshal frontier params: %v", err)
	}
	result, rpcErr = h.workspaceTensionFrontier(ctx, rawFrontier)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionFrontier rpc error: %+v", rpcErr)
	}
	frontierPayload := result.(map[string]any)
	frontierItems, ok := frontierPayload["items"].([]sqlite.TensionFrontierItem)
	if !ok || len(frontierItems) == 0 {
		t.Fatalf("unexpected frontier payload %+v", frontierPayload)
	}
	for _, item := range frontierItems {
		if item.ReviewStatus != "CONFIRMED" {
			t.Fatalf("expected CONFIRMED review status in frontier, got %+v", frontierItems)
		}
	}
}

func TestWorkspaceTensionRuntimeEventsExposeRichPayload(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := testAuthContext("ws-instrumentation-rpc", "human", "dashboard")
	scenario := seedInstrumentationRPCScenario(t, ctx, store)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "# Incident\nDeploy failed.\n\n## Fix Plan\nPatch and verify.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc for segment refs: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-instrumentation-runtime-rich",
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.primaryTaskID,
		Title:        "Runtime Richness Artifact",
		ArtifactRef:  "artifact://instrumentation-runtime-rich",
		Kind:         "note",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-b",
		MetadataJSON: `{"content":"# Timeline\n\nDeploy blocked.\n\n## Decision\nWait for operator approval."}`,
	}); err != nil {
		t.Fatalf("create workspace artifact for segment refs: %v", err)
	}

	ch := h.GetEventBus().Subscribe(scenario.workspaceID)
	defer h.GetEventBus().Unsubscribe(scenario.workspaceID, ch)

	rawRefresh, err := json.Marshal(workspaceTensionRefreshParams{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "dashboard",
		Limit:        100,
		ClusterLimit: 10,
	})
	if err != nil {
		t.Fatalf("marshal refresh params: %v", err)
	}
	result, rpcErr := h.workspaceTensionRefresh(ctx, rawRefresh)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionRefresh rpc error: %+v", rpcErr)
	}
	refreshPayload := result.(map[string]any)
	refresh := refreshPayload["refresh"].(sqlite.TensionRefreshResult)
	if len(refresh.Events) == 0 {
		t.Fatalf("expected refresh to emit tension runtime events")
	}
	for range refresh.Events {
		_ = nextEvent(t, ch)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(refresh.Events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode refresh payload: %v", err)
	}
	if payload["tension_id"] == "" || payload["proto_cluster_id"] == "" || payload["tension_type"] == "" {
		t.Fatalf("expected identity fields in refresh payload, got %+v", payload)
	}
	if payload["typed_event_type"] != "TENSION_UPDATE" {
		t.Fatalf("expected typed_event_type in refresh payload, got %+v", payload)
	}
	if payload["event_kind"] != refresh.Events[0].EventType {
		t.Fatalf("expected event_kind in refresh payload, got %+v", payload)
	}
	if refs, ok := payload["evidence_refs"].([]any); !ok || len(refs) == 0 {
		t.Fatalf("expected evidence_refs in refresh payload, got %+v", payload)
	}
	if refs, ok := payload["segment_refs"].([]any); !ok || len(refs) == 0 {
		t.Fatalf("expected segment_refs in refresh payload, got %+v", payload)
	}
	if refs, ok := payload["constraint_refs"].([]any); !ok || len(refs) == 0 {
		t.Fatalf("expected constraint_refs in refresh payload, got %+v", payload)
	}

	rawList, err := json.Marshal(workspaceTensionListParams{
		WorkspaceID: scenario.workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr = h.workspaceTensionList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspaceTensionList rpc error: %+v", rpcErr)
	}
	items := result.(map[string]any)["items"].([]sqlite.TensionRecord)
	primary := requireTensionRecordByType(t, items, "bottleneck")

	_ = requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionResolve, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "resolve before archive",
	}, "tension.resolved")

	archive := requireTensionMutation(t, ctx, h, ch, store, h.workspaceTensionArchive, workspaceTensionLifecycleParams{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "retire for payload assertion",
	}, "tension.archived")

	var archivePayload map[string]any
	if err := json.Unmarshal([]byte(archive.Event.PayloadJSON), &archivePayload); err != nil {
		t.Fatalf("decode archive payload: %v", err)
	}
	if archivePayload["event_kind"] != "tension.archived" {
		t.Fatalf("expected archive payload event_kind, got %+v", archivePayload)
	}
	if archivePayload["reason"] != "retire for payload assertion" {
		t.Fatalf("expected archive payload reason, got %+v", archivePayload)
	}
	if archivePayload["typed_event_type"] != "TENSION_UPDATE" {
		t.Fatalf("expected typed_event_type in archive payload, got %+v", archivePayload)
	}
}

func requireTensionRecordByType(t *testing.T, items []sqlite.TensionRecord, tensionType string) sqlite.TensionRecord {
	t.Helper()

	for _, item := range items {
		if item.TensionType == tensionType {
			return item
		}
	}
	t.Fatalf("tension with type %s not found in %+v", tensionType, items)
	return sqlite.TensionRecord{}
}

func requireTensionMutation(
	t *testing.T,
	ctx context.Context,
	h *Handler,
	ch <-chan EventMessage,
	store *sqlite.Store,
	fn func(context.Context, json.RawMessage) (any, *RPCError),
	params workspaceTensionLifecycleParams,
	wantEventType string,
) sqlite.TensionMutationResult {
	t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal tension mutation params: %v", err)
	}
	result, rpcErr := fn(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("tension mutation rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected mutation result type %T", result)
	}
	tension, ok := payload["tension"].(sqlite.TensionRecord)
	if !ok {
		t.Fatalf("unexpected tension payload type %T", payload["tension"])
	}
	event, ok := payload["event"].(sqlite.RuntimeEventRecord)
	if !ok {
		t.Fatalf("unexpected event payload type %T", payload["event"])
	}
	if event.EventType != wantEventType {
		t.Fatalf("expected event %s, got %+v", wantEventType, event)
	}

	live := nextEvent(t, ch)
	assertLiveEventMirrorsRuntimeEvent(t, live, event, "")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), event.PayloadJSON)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: params.WorkspaceID,
		EventType:   wantEventType,
		EntityType:  "tension",
		EntityID:    params.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension runtime events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected persisted runtime event %s for tension %s", wantEventType, params.TensionID)
	}
	assertServerWorkspaceTensionRuntimePromptContext(t, event, serverWorkspaceTensionSurfaceForEventType(wantEventType), params.WorkspaceID, params.TensionID, "human", params.ActorID, map[string]string{
		"event_kind": wantEventType,
		"actor_type": "operator",
		"actor_id":   params.ActorID,
	})

	return sqlite.TensionMutationResult{
		Tension: tension,
		Event:   event,
	}
}

func assertServerWorkspaceTensionRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantTensionID, wantPrincipalType, wantPrincipalID string, extra map[string]string) {
	t.Helper()

	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace tension prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_tension_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"principal_type":                     wantPrincipalType,
		"principal_id":                       wantPrincipalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	if wantTensionID != "" {
		expected["tension_id"] = wantTensionID
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("workspace tension prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace tension prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func serverWorkspaceTensionSurfaceForEventType(eventType string) string {
	switch eventType {
	case "tension.confirmed":
		return "workspace.tension.confirm"
	case "tension.discarded":
		return "workspace.tension.discard"
	case "tension.archived":
		return "workspace.tension.archive"
	case "tension.resolved":
		return "workspace.tension.resolve"
	case "tension.dormant":
		return "workspace.tension.dormant"
	case "tension.dependency.added":
		return "workspace.tension.add.dependency"
	case "tension.dependency.removed":
		return "workspace.tension.remove.dependency"
	case "tension.agent.attached":
		return "workspace.tension.agent.attach"
	case "tension.agent.detached":
		return "workspace.tension.agent.detach"
	default:
		return "workspace.tension.refresh"
	}
}

func seedServerMetaTensionCycle(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()

	now := "2026-04-22T00:00:00Z"
	for _, rec := range []struct {
		id      string
		base    int
		surface int
	}{
		{id: "srv-meta-t1", base: 60, surface: 30},
		{id: "srv-meta-t2", base: 50, surface: 45},
		{id: "srv-meta-t3", base: 25, surface: 10},
	} {
		if _, err := store.DB().ExecContext(ctx, `
			INSERT INTO workspace_tensions (
				tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status,
				title, summary, anchor_kind, anchor_ref, base_score, surface_score, created_at, updated_at
			) VALUES (?, ?, ?, 'gap', 'ACTIVE', 'PENDING', ?, ?, 'server_test', ?, ?, ?, ?, ?)
			ON CONFLICT(tension_id) DO UPDATE SET lifecycle_state = excluded.lifecycle_state`,
			rec.id, workspaceID, "server-meta-cycle", "Server meta "+rec.id, "Server SCC cycle member", rec.id, rec.base, rec.surface, now, now,
		); err != nil {
			t.Fatalf("insert server meta tension %s: %v", rec.id, err)
		}
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "srv-meta-t1", "srv-meta-t2", "BLOCKS"); err != nil {
		t.Fatalf("add server meta dependency t1->t2: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "srv-meta-t2", "srv-meta-t3", "BLOCKS"); err != nil {
		t.Fatalf("add server meta dependency t2->t3: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "srv-meta-t3", "srv-meta-t1", "BLOCKS"); err != nil {
		t.Fatalf("add server meta dependency t3->t1: %v", err)
	}
}

func countServerMetaTensionRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tensions WHERE workspace_id = ? AND tension_type = 'meta-tension'`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count server meta tension rows: %v", err)
	}
	return count
}

func countServerTensionDependencyEdges(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, dependsOnTensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, dependsOnTensionID).Scan(&count); err != nil {
		t.Fatalf("count server tension dependency edges: %v", err)
	}
	return count
}

func countServerTensionCoalitions(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ?`, workspaceID, tensionID).Scan(&count); err != nil {
		t.Fatalf("count server tension coalitions: %v", err)
	}
	return count
}

func countServerTensionCoalitionMembers(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, coalitionID, agentID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_coalition_members WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, workspaceID, coalitionID, agentID).Scan(&count); err != nil {
		t.Fatalf("count server tension coalition members: %v", err)
	}
	return count
}
