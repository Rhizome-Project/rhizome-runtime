package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestRefreshTensionsUsesConcreteDocAndTextArtifactSegmentRefsWhenAvailable(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	scenario := seedTensionSegmentScenario(t, ctx, store, tensionSegmentScenarioInput{
		Suffix:              "concrete-segments",
		DocContent:          "# Incident\nDeploy failed.\n\n## Fix Plan\nPatch the rollout and verify the queue.",
		ArtifactContentType: "text/markdown",
		ArtifactMetadataJSON: mustJSONString(t, map[string]any{
			"content": "# Timeline\n\nBlocked on rollout.\n\n## Operator Notes\nConfirm the deploy.",
			"text":    "# Timeline\n\nBlocked on rollout.\n\n## Operator Notes\nConfirm the deploy.",
		}),
	})

	first, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("refresh tensions for concrete segment refs: %v", err)
	}
	if len(first.Events) == 0 {
		t.Fatalf("expected refresh to emit tension runtime events, got %+v", first)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after concrete segment refresh: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	detail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get tension detail for concrete segments: %v", err)
	}

	assertUniqueStringValues(t, detail.Tension.SegmentRefs)
	assertUniqueStringValues(t, detail.Tension.ConstraintRefs)

	docPrefix := "workspace_doc:" + scenario.workspaceID + "/" + scenario.docKey + "#"
	docRefs := segmentRefsWithPrefix(detail.Tension.SegmentRefs, docPrefix)
	if len(docRefs) == 0 {
		t.Fatalf("expected doc-backed segment refs for %s, got %+v", scenario.docKey, detail.Tension.SegmentRefs)
	}
	if !hasNonRootRef(docRefs) {
		t.Fatalf("expected headed workspace doc to surface a concrete non-root segment ref, got %+v", docRefs)
	}

	artifactPrefix := "artifact:" + scenario.workspaceID + "/" + scenario.artifactRef + "#"
	artifactRefs := segmentRefsWithPrefix(detail.Tension.SegmentRefs, artifactPrefix)
	if len(artifactRefs) == 0 {
		t.Fatalf("expected artifact-backed segment refs for %s, got %+v", scenario.artifactRef, detail.Tension.SegmentRefs)
	}
	if !hasNonRootRef(artifactRefs) {
		t.Fatalf("expected text artifact to surface a concrete non-root segment ref, got %+v", artifactRefs)
	}

	expectedQueueRef := "queue:" + sessionOperatorQueueKey(scenario.sessionID, "BLOCKER")
	if !containsStringValue(detail.Tension.ConstraintRefs, expectedQueueRef) {
		t.Fatalf("expected blocker queue constraint ref %s, got %+v", expectedQueueRef, detail.Tension.ConstraintRefs)
	}
	for _, forbiddenPrefix := range []string{"doc:", "artifact:"} {
		if hasConstraintPrefix(detail.Tension.ConstraintRefs, forbiddenPrefix) {
			t.Fatalf("expected structural refs to stay out of constraint_refs, got %+v", detail.Tension.ConstraintRefs)
		}
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.detected",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension.detected runtime events for concrete segments: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one tension.detected event, got %+v", events)
	}
	payload := decodeJSONMapForSegmentTest(t, events[0].PayloadJSON)
	assertPayloadStringSliceUnique(t, payload, "segment_refs")
	assertPayloadStringSliceUnique(t, payload, "constraint_refs")
	if !hasNonRootRef(segmentRefsWithPrefix(payloadStringSlice(t, payload, "segment_refs"), docPrefix)) {
		t.Fatalf("expected tension.detected payload to preserve concrete doc segment refs, got %+v", payload)
	}
	if !hasNonRootRef(segmentRefsWithPrefix(payloadStringSlice(t, payload, "segment_refs"), artifactPrefix)) {
		t.Fatalf("expected tension.detected payload to preserve concrete artifact segment refs, got %+v", payload)
	}
	if !containsStringValue(payloadStringSlice(t, payload, "constraint_refs"), expectedQueueRef) {
		t.Fatalf("expected tension.detected payload to preserve queue constraint ref %s, got %+v", expectedQueueRef, payload)
	}
	for _, forbiddenPrefix := range []string{"doc:", "artifact:"} {
		if hasConstraintPrefix(payloadStringSlice(t, payload, "constraint_refs"), forbiddenPrefix) {
			t.Fatalf("expected structural refs to stay out of tension.detected constraint_refs, got %+v", payload)
		}
	}

	second, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	})
	if err != nil {
		t.Fatalf("second refresh tensions for concrete segment refs: %v", err)
	}
	if second.CreatedCount != 0 {
		t.Fatalf("expected repeated refresh not to create duplicate tensions, got %+v", second)
	}
	secondDetail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get tension detail after repeated refresh: %v", err)
	}
	assertUniqueStringValues(t, secondDetail.Tension.SegmentRefs)
	assertUniqueStringValues(t, secondDetail.Tension.ConstraintRefs)
	if len(secondDetail.Tension.SegmentRefs) != len(detail.Tension.SegmentRefs) {
		t.Fatalf("expected repeated refresh not to duplicate segment refs, before=%+v after=%+v", detail.Tension.SegmentRefs, secondDetail.Tension.SegmentRefs)
	}
	if len(secondDetail.Tension.ConstraintRefs) != len(detail.Tension.ConstraintRefs) {
		t.Fatalf("expected repeated refresh not to duplicate constraint refs, before=%+v after=%+v", detail.Tension.ConstraintRefs, secondDetail.Tension.ConstraintRefs)
	}
}

func TestRefreshTensionsFallsBackToRootSegmentsForUnstructuredDocAndNonTextArtifact(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	scenario := seedTensionSegmentScenario(t, ctx, store, tensionSegmentScenarioInput{
		Suffix:               "root-fallback",
		DocContent:           "short operator note",
		ArtifactContentType:  "application/octet-stream",
		ArtifactMetadataJSON: mustJSONString(t, map[string]any{"bytes": 42}),
	})

	if _, err := store.RefreshTensions(ctx, TensionRefreshInput{
		WorkspaceID:  scenario.workspaceID,
		ActorID:      "tests",
		Limit:        100,
		ClusterLimit: 20,
	}); err != nil {
		t.Fatalf("refresh tensions for root fallback segments: %v", err)
	}

	items, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after root fallback refresh: %v", err)
	}
	primary := requireStorageTensionByType(t, items, "bottleneck")
	detail, err := store.GetTension(ctx, scenario.workspaceID, primary.TensionID)
	if err != nil {
		t.Fatalf("get tension detail for root fallback: %v", err)
	}

	assertUniqueStringValues(t, detail.Tension.SegmentRefs)
	docRefs := segmentRefsWithPrefix(detail.Tension.SegmentRefs, "workspace_doc:"+scenario.workspaceID+"/"+scenario.docKey+"#")
	if len(docRefs) == 0 {
		t.Fatalf("expected root doc segment ref, got %+v", detail.Tension.SegmentRefs)
	}
	if !allRootRefs(docRefs) {
		t.Fatalf("expected unstructured workspace doc to fall back to root segment refs, got %+v", docRefs)
	}

	artifactRefs := segmentRefsWithPrefix(detail.Tension.SegmentRefs, "artifact:"+scenario.workspaceID+"/"+scenario.artifactRef+"#")
	if len(artifactRefs) == 0 {
		t.Fatalf("expected root artifact segment ref, got %+v", detail.Tension.SegmentRefs)
	}
	if !allRootRefs(artifactRefs) {
		t.Fatalf("expected non-text artifact to fall back to root segment refs, got %+v", artifactRefs)
	}
}

type tensionSegmentScenarioInput struct {
	Suffix               string
	DocContent           string
	ArtifactContentType  string
	ArtifactMetadataJSON string
}

type tensionSegmentScenario struct {
	workspaceID string
	taskID      string
	sessionID   string
	docKey      string
	artifactRef string
}

func seedTensionSegmentScenario(t *testing.T, ctx context.Context, store *Store, input tensionSegmentScenarioInput) tensionSegmentScenario {
	t.Helper()

	suffix := strings.TrimSpace(input.Suffix)
	if suffix == "" {
		suffix = "segment"
	}
	scenario := tensionSegmentScenario{
		workspaceID: "ws-tension-segment-" + suffix,
		taskID:      "task-tension-segment-" + suffix,
		sessionID:   "sess-tension-segment-" + suffix,
		docKey:      "doc-tension-segment-" + suffix,
		artifactRef: "artifact://tension-segment-" + suffix,
	}

	setupInstrumentationInternalWorkspace(t, ctx, store, scenario.workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "node-tension-segment-"+suffix)
	claimInternalTaskForSessionStart(t, ctx, store, scenario.workspaceID, scenario.taskID, "agent-a")

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Segment Scenario",
		Content:     input.DocContent,
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc for segment scenario: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		ArtifactID:   "artifact-tension-segment-" + suffix,
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Title:        "Segment Artifact",
		ArtifactRef:  scenario.artifactRef,
		Kind:         "document",
		ContentType:  input.ArtifactContentType,
		CreatedBy:    "agent-a",
		MetadataJSON: input.ArtifactMetadataJSON,
	}); err != nil {
		t.Fatalf("create workspace artifact for segment scenario: %v", err)
	}

	keepTrue := true
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:         model.SessionEventStart,
		WorkspaceID:       scenario.workspaceID,
		SessionID:         scenario.sessionID,
		AgentID:           "agent-a",
		TaskID:            scenario.taskID,
		Summary:           "start segment scenario",
		OwnerScope:        "task/session",
		KeepSessionActive: &keepTrue,
		RelatedDocKeys: []string{
			scenario.docKey,
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record segment scenario session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     "agent-a",
		TaskID:      scenario.taskID,
		Summary:     "blocked segment scenario",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve segment refresh"},
		},
		RelatedDocKeys: []string{
			scenario.docKey,
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
			{Ref: scenario.artifactRef},
		},
	})
	if err != nil {
		t.Fatalf("record segment scenario blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue for segment scenario: %v", err)
	}

	return scenario
}

func assertUniqueStringValues(t *testing.T, items []string) {
	t.Helper()
	seen := map[string]struct{}{}
	for _, item := range items {
		if _, ok := seen[item]; ok {
			t.Fatalf("expected unique string values, duplicate %q in %+v", item, items)
		}
		seen[item] = struct{}{}
	}
}

func segmentRefsWithPrefix(items []string, prefix string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return out
}

func hasNonRootRef(items []string) bool {
	for _, item := range items {
		if !strings.HasSuffix(item, "#root") {
			return true
		}
	}
	return false
}

func hasConstraintPrefix(items []string, prefix string) bool {
	for _, item := range items {
		if strings.HasPrefix(strings.TrimSpace(item), prefix) {
			return true
		}
	}
	return false
}

func allRootRefs(items []string) bool {
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if !strings.HasSuffix(item, "#root") {
			return false
		}
	}
	return true
}

func containsStringValue(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func decodeJSONMapForSegmentTest(t *testing.T, payload string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal([]byte(payload), &out); err != nil {
		t.Fatalf("decode json payload: %v", err)
	}
	return out
}

func payloadStringSlice(t *testing.T, payload map[string]any, key string) []string {
	t.Helper()
	raw, ok := payload[key]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		t.Fatalf("expected payload key %s to be []any, got %T (%+v)", key, raw, raw)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("expected payload key %s entry to be string, got %T (%+v)", key, item, item)
		}
		out = append(out, text)
	}
	return out
}

func assertPayloadStringSliceUnique(t *testing.T, payload map[string]any, key string) {
	t.Helper()
	assertUniqueStringValues(t, payloadStringSlice(t, payload, key))
}
