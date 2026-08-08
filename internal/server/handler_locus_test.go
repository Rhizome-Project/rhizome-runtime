package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceInstrumentationLocusBundleReturnsBundledReadSideContext(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	workspaceID := "ws-handler-locus"
	taskID := "task-locus"
	sessionID := "sess-locus"
	docKey := "task.task-locus"
	artifactRef := "doc:task-locus"

	seedHandlerAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"agent-a", "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, workspaceID, taskID, "high")

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Task Locus",
		Content:     "Task locus context",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Task Artifact",
		ArtifactRef: artifactRef,
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-a",
		Summary:     "claim locus task",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.CreateAgentSessionWithRuntimeEvent(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     "agent-a",
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     "agent-a",
		TaskID:      taskID,
		Summary:     "blocked on approval",
		BlockedOn: []model.AgentUpdateBlockedRef{
			{Kind: "human_input", Detail: "approve locus"},
		},
		RelatedDocKeys:      []string{docKey},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{{Ref: artifactRef}},
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: workspaceID,
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}

	tensions, _ := store.ListTensions(ctx, sqlite.TensionFilter{WorkspaceID: workspaceID})
	t.Logf("DEBUG: Found %d tensions.", len(tensions))
	for _, tns := range tensions {
		t.Logf("DEBUG: Tension %s (Anchor: %s) ReviewStatus: '%s' Lifecycle: '%s' SegmentRefs: %v", tns.TensionID, tns.AnchorRef, tns.ReviewStatus, tns.LifecycleState, tns.SegmentRefs)
	}

	raw, err := json.Marshal(workspaceInstrumentationLocusParams{
		WorkspaceID:   workspaceID,
		AgentID:       "agent-a",
		TaskID:        taskID,
		SessionID:     sessionID,
		DocKeys:       []string{docKey},
		ArtifactRefs:  []string{artifactRef},
		FrontierLimit: 2,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceInstrumentationLocusBundle(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationLocusBundle rpc error: %+v", rpcErr)
	}

	payload := result.(map[string]any)
	bundle, ok := payload["bundle"].(sqlite.InstrumentationLocusBundle)
	if !ok {
		t.Fatalf("unexpected bundle type %T", payload["bundle"])
	}
	if !bundle.Resolved || bundle.ProtoClusterID == "" {
		t.Fatalf("expected resolved bundle, got %+v", bundle)
	}
	if bundle.TimeAuthority.WorkspaceID != workspaceID || bundle.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected locus bundle to expose workspace time authority, got %+v", bundle.TimeAuthority)
	}
	if bundle.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected locus bundle generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", bundle.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.Control == nil || bundle.Corridor == nil || bundle.ControlState == nil {
		t.Fatalf("expected bundled read-side surfaces, got %+v", bundle)
	}
	if bundle.MemoryCoherence == nil || bundle.MemoryCoherence.AgentID != "agent-a" {
		t.Fatalf("expected bundled memory coherence detail, got %+v", bundle)
	}
	if bundle.CorridorOwnership == nil || bundle.CorridorOwnership.Cluster.ProtoClusterID == "" {
		t.Fatalf("expected bundled corridor ownership digest, got %+v", bundle)
	}
	if bundle.CorridorBoundary == nil || bundle.CorridorBoundary.Cluster.ProtoClusterID == "" {
		t.Fatalf("expected bundled corridor boundary digest, got %+v", bundle)
	}
	if bundle.CorridorAuthority == nil || bundle.CorridorAuthority.Task.TaskID != taskID {
		t.Fatalf("expected bundled task-first corridor authority detail, got %+v", bundle)
	}
	if bundle.SegmentReport == nil || len(bundle.SegmentReport.Segments) == 0 {
		t.Fatalf("expected bundled segment report, got %+v", bundle)
	}
	if bundle.SegmentReport.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected bundled segment report generated_at to mirror locus time authority reference_at, got generated_at=%q reference_at=%q", bundle.SegmentReport.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.SegmentReport.Segments[0].GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected bundled segment generated_at to mirror locus time authority reference_at, got %+v", bundle.SegmentReport.Segments[0])
	}
	if len(bundle.RelatedSegmentRefs) == 0 {
		t.Fatalf("expected bundled related segment refs, got %+v", bundle)
	}
	if len(bundle.Frontier) == 0 || bundle.DominantTension == nil {
		t.Fatalf("expected scoped tension surfaces, got %+v", bundle)
	}
}

func TestWorkspaceInstrumentationLocusBundleReturnsStableShadowOnlySidecar(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	scenario := seedHandlerLocusSidecarScenario(t, ctx, store, "stable")
	result, rpcErr := h.workspaceInstrumentationLocusBundle(ctx, mustJSONRaw(workspaceInstrumentationLocusParams{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationLocusBundle rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected locus bundle result type %T", result)
	}
	bundle, ok := payload["bundle"].(sqlite.InstrumentationLocusBundle)
	if !ok {
		t.Fatalf("unexpected locus bundle payload type %T", payload["bundle"])
	}
	if !bundle.Resolved || bundle.Control == nil || bundle.ControlState == nil || bundle.MemoryCoherence == nil {
		t.Fatalf("expected stable sidecar bundle to resolve control/state/coherence surfaces, got %+v", bundle)
	}
	if bundle.GeneratedAt != bundle.TimeAuthority.ReferenceAt {
		t.Fatalf("expected stable sidecar locus bundle generated_at to mirror time authority reference_at, got generated_at=%q reference_at=%q", bundle.GeneratedAt, bundle.TimeAuthority.ReferenceAt)
	}
	if bundle.ControlState.State.State.CurrentMode != "STEADY" || bundle.ControlState.State.State.AttentionBand != "STEADY" {
		t.Fatalf("expected stable sidecar bundle to stay steady, got %+v", bundle.ControlState.State.State)
	}
	if bundle.MemoryCoherence.CoherenceBandHint != "STABLE" || bundle.MemoryCoherence.NeedsAttention {
		t.Fatalf("expected stable sidecar bundle to stay in stable coherence band, got %+v", bundle.MemoryCoherence)
	}
}

func TestWorkspaceInstrumentationLocusBundleRejectsMissingWorkspaceID(t *testing.T) {
	t.Parallel()

	h := NewHandler(newServerTestStore(t))
	if _, rpcErr := h.workspaceInstrumentationLocusBundle(context.Background(), mustJSONRaw(workspaceInstrumentationLocusParams{
		AgentID: "agent-a",
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected missing workspace_id invalid params error, got %+v", rpcErr)
	}
}

type locusSidecarServerScenario struct {
	workspaceID string
	taskID      string
	docKey      string
	artifactRef string
	sessionID   string
	agentID     string
}

func seedHandlerLocusSidecarScenario(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) locusSidecarServerScenario {
	t.Helper()

	scenario := locusSidecarServerScenario{
		workspaceID: "ws-handler-locus-sidecar-" + suffix,
		taskID:      "task-handler-locus-sidecar-" + suffix,
		docKey:      "locus-sidecar-doc-" + suffix,
		artifactRef: "artifact://handler-locus-sidecar-" + suffix,
		sessionID:   "sess-handler-locus-sidecar-" + suffix,
		agentID:     "agent-a",
	}

	seedHandlerAgentWorkWorkspace(t, ctx, store, scenario.workspaceID, []string{scenario.agentID, "agent-b"})
	createHandlerAgentWorkTask(t, ctx, store, scenario.workspaceID, scenario.taskID, "normal")
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		AgentID:     scenario.agentID,
		Summary:     "fixture claim before task-bound session",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.docKey,
		Title:       "Handler Locus Sidecar Runbook",
		Content:     "Stable locus sidecar scenario",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Handler Locus Sidecar Artifact",
		ArtifactRef: scenario.artifactRef,
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
		CreatedBy:   scenario.agentID,
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   scenario.sessionID,
		AgentID:     scenario.agentID,
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: scenario.workspaceID,
		SessionID:   scenario.sessionID,
		AgentID:     scenario.agentID,
		TaskID:      scenario.taskID,
		Summary:     "stable locus sidecar session",
		OwnerScope:  "task/session",
		RelatedDocKeys: []string{
			scenario.docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: scenario.artifactRef},
		},
	}); err != nil {
		t.Fatalf("record agent session coordination: %v", err)
	}
	return scenario
}
