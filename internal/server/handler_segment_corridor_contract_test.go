package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceSegmentListSupportsSegmentRefFilteringAndGetRejectsUnknownSegment(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-workspace-segment-contracts"
	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "# Incident\nDeploy failed.\n\n## Fix Plan\nPatch and verify.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc for segment filter contract: %v", err)
	}

	rawList, err := json.Marshal(workspaceSegmentListParams{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	result, rpcErr := h.workspaceSegmentList(ctx, rawList)
	if rpcErr != nil {
		t.Fatalf("workspace.segment.list rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list result type %T", result)
	}
	report, ok := payload["report"].(sqlite.WorkspaceSegmentReport)
	if !ok {
		t.Fatalf("unexpected workspace.segment.list report type %T", payload["report"])
	}
	target := requireServerNonRootWorkspaceSegment(t, report.Segments, "workspace_doc")

	rawFiltered, err := json.Marshal(workspaceSegmentListParams{
		WorkspaceID: workspaceID,
		SegmentRef:  target.SegmentRef,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal segment_ref list params: %v", err)
	}
	result, rpcErr = h.workspaceSegmentList(ctx, rawFiltered)
	if rpcErr != nil {
		t.Fatalf("workspace.segment.list segment_ref rpc error: %+v", rpcErr)
	}
	filteredPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected filtered workspace.segment.list result type %T", result)
	}
	filteredReport, ok := filteredPayload["report"].(sqlite.WorkspaceSegmentReport)
	if !ok {
		t.Fatalf("unexpected filtered workspace.segment.list report type %T", filteredPayload["report"])
	}
	if len(filteredReport.Segments) != 1 || filteredReport.Segments[0].SegmentRef != target.SegmentRef {
		t.Fatalf("expected segment_ref filter to return exactly one segment, got %+v", filteredReport.Segments)
	}

	rawGet, err := json.Marshal(workspaceSegmentGetParams{
		WorkspaceID: workspaceID,
		SegmentRef:  "workspace_doc:" + workspaceID + "/missing#root",
	})
	if err != nil {
		t.Fatalf("marshal unknown get params: %v", err)
	}
	if _, rpcErr := h.workspaceSegmentGet(ctx, rawGet); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected workspace.segment.get to reject unknown segment_ref, got %+v", rpcErr)
	}
}

func TestWorkspaceInstrumentationCorridorAuthorityRPCSurfacesBasisStatesAndInactiveAuthoredVisibility(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-corridor-authority-contracts"
	createServerTaskClassWorkspace(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "developer",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register corridor authority rpc agent: %v", err)
	}

	const (
		authoredFreshTaskID = "task-corridor-authority-rpc-authored-fresh"
		authoredStaleTaskID = "task-corridor-authority-rpc-authored-stale"
		derivedOnlyTaskID   = "task-corridor-authority-rpc-derived-only"
		noBasisTaskID       = "task-corridor-authority-rpc-no-basis"
	)

	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          authoredFreshTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout authority",
		Description:     "Explicit authored class evidence should stay visible even before runtime activity.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          authoredStaleTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Validate proof corridor",
		Description:     "Explicit authored class can go stale while the task stays active in instrumentation.",
		TaskKind:        model.TaskKindCoordination,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassProof,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       derivedOnlyTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Explore remediation options",
		Description:  "Research the best deployment path before execution.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateResearch,
	})
	createServerTaskClassTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       noBasisTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Follow-up",
		Description:  "Continue work.",
		TaskKind:     model.TaskKindExecution,
		TaskTemplate: model.TaskTemplateGeneric,
	})

	if _, err := store.DB().ExecContext(
		ctx,
		`UPDATE tasks SET task_class_updated_at = ? WHERE task_id = ?`,
		time.Now().UTC().Add(-96*time.Hour).Format(time.RFC3339Nano),
		authoredStaleTaskID,
	); err != nil {
		t.Fatalf("stale authored task_class_updated_at: %v", err)
	}

	recordServerTaskClassRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    authoredStaleTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		TaskID:      authoredStaleTaskID,
		PayloadJSON: `{"task_id":"` + authoredStaleTaskID + `"}`,
	})
	recordServerTaskClassRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    derivedOnlyTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		TaskID:      derivedOnlyTaskID,
		PayloadJSON: `{"task_id":"` + derivedOnlyTaskID + `"}`,
	})

	rawReport, err := json.Marshal(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("marshal corridor authority report params: %v", err)
	}
	result, rpcErr := h.workspaceInstrumentationCorridorAuthorityReport(ctx, rawReport)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityReport rpc error: %+v", rpcErr)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected corridor authority report result type %T", result)
	}
	report, ok := payload["report"].(sqlite.CorridorAuthorityReport)
	if !ok {
		t.Fatalf("unexpected corridor authority report payload type %T", payload["report"])
	}
	if report.Workspace.AuthoredFreshCount != 1 || report.Workspace.AuthoredStaleCount != 1 || report.Workspace.DerivedOnlyCount != 1 || report.Workspace.NoBasisCount != 1 {
		t.Fatalf("unexpected corridor authority basis-state counts via rpc: %+v", report.Workspace)
	}
	if report.Workspace.VisibleTaskCount != 2 || report.Workspace.InactiveAuthoredTaskCount != 1 {
		t.Fatalf("unexpected corridor authority visibility counts via rpc: %+v", report.Workspace)
	}

	authoredFresh := requireServerCorridorAuthorityTask(t, report.Tasks, authoredFreshTaskID)
	if authoredFresh.BasisState != "AUTHORED_FRESH" || !authoredFresh.BasisAuthoritative || authoredFresh.VisibleInInstrumentation {
		t.Fatalf("expected inactive authored-fresh task via rpc, got %+v", authoredFresh)
	}

	authoredStale := requireServerCorridorAuthorityTask(t, report.Tasks, authoredStaleTaskID)
	if authoredStale.BasisState != "AUTHORED_STALE" || !authoredStale.VisibleInInstrumentation || len(authoredStale.ActiveProtoClusterIDs) != 1 {
		t.Fatalf("expected visible authored-stale task via rpc, got %+v", authoredStale)
	}

	derivedOnly := requireServerCorridorAuthorityTask(t, report.Tasks, derivedOnlyTaskID)
	if derivedOnly.BasisState != "DERIVED_ONLY" || derivedOnly.BasisAuthoritative || derivedOnly.TaskClassHint != model.TaskClassExploration {
		t.Fatalf("expected derived-only task via rpc, got %+v", derivedOnly)
	}

	noBasis := requireServerCorridorAuthorityTask(t, report.Tasks, noBasisTaskID)
	if noBasis.BasisState != "NO_BASIS" || noBasis.CorridorLookup.LookupStatus != "NO_MATCH" {
		t.Fatalf("expected no-basis task via rpc, got %+v", noBasis)
	}

	rawTask, err := json.Marshal(workspaceInstrumentationCorridorAuthorityParams{
		WorkspaceID: workspaceID,
		TaskID:      authoredStaleTaskID,
	})
	if err != nil {
		t.Fatalf("marshal corridor authority task params: %v", err)
	}
	result, rpcErr = h.workspaceInstrumentationCorridorAuthorityTask(ctx, rawTask)
	if rpcErr != nil {
		t.Fatalf("workspaceInstrumentationCorridorAuthorityTask rpc error: %+v", rpcErr)
	}
	taskPayload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected corridor authority task result type %T", result)
	}
	detail, ok := taskPayload["detail"].(sqlite.CorridorAuthorityTaskDetail)
	if !ok {
		t.Fatalf("unexpected corridor authority task detail type %T", taskPayload["detail"])
	}
	if detail.Task.TaskID != authoredStaleTaskID || detail.Task.BasisState != "AUTHORED_STALE" {
		t.Fatalf("expected authored-stale task detail via rpc, got %+v", detail.Task)
	}
	if len(detail.Clusters) != 1 || detail.Clusters[0].ProtoClusterID != "task:"+workspaceID+"/"+authoredStaleTaskID {
		t.Fatalf("expected authored-stale task detail to expose active cluster linkage, got %+v", detail.Clusters)
	}
}

func recordServerTaskClassRuntimeEvent(t *testing.T, ctx context.Context, store *sqlite.Store, input sqlite.RuntimeEventInput) {
	t.Helper()
	if input.EventID == "" {
		input.EventID = "rtev-server-task-class-" + strings.ReplaceAll(time.Now().UTC().Format(time.RFC3339Nano), ":", "-") + "-" + strings.ReplaceAll(strings.TrimSpace(input.TaskID), "/", "-")
	}
	if input.ActorType == "" {
		input.ActorType = "agent"
	}
	if _, err := store.RecordRuntimeEvent(ctx, input); err != nil {
		t.Fatalf("record runtime event: %v", err)
	}
}
