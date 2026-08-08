package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBuildCorridorAuthorityReportSurfacesBasisStatesVisibilityAndInactiveAuthoredTasks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-corridor-authority-state-matrix"
	setupCorridorWorkspace(t, ctx, store, workspaceID, "agent-a")

	const (
		authoredFreshTaskID = "task-corridor-authority-authored-fresh"
		authoredStaleTaskID = "task-corridor-authority-authored-stale"
		derivedOnlyTaskID   = "task-corridor-authority-derived-only"
		noBasisTaskID       = "task-corridor-authority-no-basis"
	)

	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          authoredFreshTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Repair rollout authority",
		Description:     "Explicit task-authored corridor authority should stay visible without runtime activity.",
		TaskKind:        model.TaskKindExecution,
		TaskTemplate:    model.TaskTemplateBugfix,
		TaskClass:       model.TaskClassIncident,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:          authoredStaleTaskID,
		OwnerUserID:     "developer",
		Priority:        "normal",
		Title:           "Validate proof corridor",
		Description:     "Explicit authored class can become stale while the task remains active in instrumentation.",
		TaskKind:        model.TaskKindCoordination,
		TaskTemplate:    model.TaskTemplateResearch,
		TaskClass:       model.TaskClassProof,
		TaskClassSource: model.TaskClassSourceExplicit,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
		TaskID:       derivedOnlyTaskID,
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Explore remediation options",
		Description:  "Research the best deployment path before execution.",
		TaskKind:     model.TaskKindCoordination,
		TaskTemplate: model.TaskTemplateResearch,
	})
	createCorridorTask(t, ctx, store, workspaceID, sqlite.TaskCreateInput{
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

	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    authoredStaleTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      authoredStaleTaskID,
		PayloadJSON: `{"task_id":"` + authoredStaleTaskID + `"}`,
	})
	recordCorridorRuntimeEvent(t, ctx, store, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "agent.update.posted",
		EntityType:  "task",
		EntityID:    derivedOnlyTaskID,
		ActorType:   "agent",
		ActorID:     "agent-a",
		AgentID:     "agent-a",
		TaskID:      derivedOnlyTaskID,
		PayloadJSON: `{"task_id":"` + derivedOnlyTaskID + `"}`,
	})

	report, err := store.BuildCorridorAuthorityReport(ctx, sqlite.CorridorAuthorityFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("build corridor authority report: %v", err)
	}
	authority, err := store.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get workspace time authority: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFieldsExternal(t, report.TimeAuthority, authority)
	if report.GeneratedAt != report.TimeAuthority.ReferenceAt {
		t.Fatalf("expected corridor authority report generated_at %q to anchor to report time authority reference_at %q", report.GeneratedAt, report.TimeAuthority.ReferenceAt)
	}

	if report.Workspace.TotalTasks != 4 {
		t.Fatalf("expected four tasks in corridor authority report, got %+v", report.Workspace)
	}
	if report.Workspace.AuthoredFreshCount != 1 || report.Workspace.AuthoredStaleCount != 1 || report.Workspace.DerivedOnlyCount != 1 || report.Workspace.NoBasisCount != 1 {
		t.Fatalf("unexpected corridor authority basis-state counts: %+v", report.Workspace)
	}
	if report.Workspace.VisibleTaskCount != 2 {
		t.Fatalf("expected two instrumented tasks in corridor authority report, got %+v", report.Workspace)
	}
	if report.Workspace.InactiveAuthoredTaskCount != 1 {
		t.Fatalf("expected one inactive authored task in corridor authority report, got %+v", report.Workspace)
	}
	if report.Workspace.BasisStateCounts["AUTHORED_FRESH"] != 1 || report.Workspace.BasisStateCounts["AUTHORED_STALE"] != 1 || report.Workspace.BasisStateCounts["DERIVED_ONLY"] != 1 || report.Workspace.BasisStateCounts["NO_BASIS"] != 1 {
		t.Fatalf("unexpected corridor authority basis-state map: %+v", report.Workspace.BasisStateCounts)
	}

	authoredFresh := requireCorridorAuthorityTask(t, report.Tasks, authoredFreshTaskID)
	if authoredFresh.BasisState != "AUTHORED_FRESH" || !authoredFresh.BasisFresh || !authoredFresh.BasisAuthoritative {
		t.Fatalf("expected authored fresh task basis, got %+v", authoredFresh)
	}
	if authoredFresh.VisibleInInstrumentation || len(authoredFresh.ActiveProtoClusterIDs) != 0 || authoredFresh.LastActivityAt != "" {
		t.Fatalf("expected authored fresh task to stay inactive in instrumentation, got %+v", authoredFresh)
	}
	if authoredFresh.CorridorLookup.LookupStatus != "CLASS_MATCH" || authoredFresh.CorridorLookup.CatalogKey != "incident" {
		t.Fatalf("expected authored fresh corridor lookup to remain authoritative, got %+v", authoredFresh.CorridorLookup)
	}

	authoredStale := requireCorridorAuthorityTask(t, report.Tasks, authoredStaleTaskID)
	if authoredStale.BasisState != "AUTHORED_STALE" || authoredStale.BasisFresh || !authoredStale.BasisAuthoritative {
		t.Fatalf("expected authored stale task basis, got %+v", authoredStale)
	}
	if !authoredStale.VisibleInInstrumentation || len(authoredStale.ActiveProtoClusterIDs) != 1 || authoredStale.LastActivityAt == "" {
		t.Fatalf("expected authored stale task to remain visible in instrumentation, got %+v", authoredStale)
	}
	if authoredStale.CorridorLookup.LookupStatus != "CLASS_MATCH" || authoredStale.CorridorLookup.CatalogKey != "proof" {
		t.Fatalf("expected authored stale corridor lookup to keep explicit class authority, got %+v", authoredStale.CorridorLookup)
	}

	derivedOnly := requireCorridorAuthorityTask(t, report.Tasks, derivedOnlyTaskID)
	if derivedOnly.BasisState != "DERIVED_ONLY" || !derivedOnly.BasisFresh || derivedOnly.BasisAuthoritative {
		t.Fatalf("expected derived-only task basis, got %+v", derivedOnly)
	}
	if derivedOnly.TaskClass != "" || derivedOnly.TaskClassSource != "" {
		t.Fatalf("expected derived-only task not to surface authored class fields, got %+v", derivedOnly)
	}
	if derivedOnly.TaskClassHint != model.TaskClassExploration || len(derivedOnly.TaskClassBasis) == 0 || derivedOnly.BasisUpdatedAt == "" {
		t.Fatalf("expected derived-only task to preserve heuristic hint and basis freshness, got %+v", derivedOnly)
	}
	if !derivedOnly.VisibleInInstrumentation || len(derivedOnly.ActiveProtoClusterIDs) != 1 {
		t.Fatalf("expected derived-only task to stay visible via instrumentation cluster, got %+v", derivedOnly)
	}
	if derivedOnly.CorridorLookup.LookupStatus == "NO_MATCH" || derivedOnly.CorridorLookup.CatalogKey != "exploration" {
		t.Fatalf("expected derived-only task to retain corridor lookup, got %+v", derivedOnly.CorridorLookup)
	}

	noBasis := requireCorridorAuthorityTask(t, report.Tasks, noBasisTaskID)
	if noBasis.BasisState != "NO_BASIS" || noBasis.BasisFresh || noBasis.BasisAuthoritative {
		t.Fatalf("expected no-basis task classification, got %+v", noBasis)
	}
	if noBasis.TaskClass != "" || noBasis.TaskClassSource != "" || noBasis.TaskClassHint != "UNKNOWN" {
		t.Fatalf("expected no-basis task not to surface authoritative or heuristic class, got %+v", noBasis)
	}
	if noBasis.CorridorLookup.LookupStatus != "NO_MATCH" || noBasis.VisibleInInstrumentation {
		t.Fatalf("expected no-basis task to remain inactive and unmatched, got %+v", noBasis)
	}

	staleDetail, err := store.BuildCorridorAuthorityTaskDetail(ctx, workspaceID, authoredStaleTaskID)
	if err != nil {
		t.Fatalf("build corridor authority task detail for stale authored task: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFieldsExternal(t, staleDetail.TimeAuthority, authority)
	if staleDetail.Task.TaskID != authoredStaleTaskID || staleDetail.Task.BasisState != "AUTHORED_STALE" {
		t.Fatalf("expected stale authored task detail to preserve basis state, got %+v", staleDetail.Task)
	}
	if len(staleDetail.Clusters) != 1 || staleDetail.Clusters[0].ProtoClusterID != "task:"+workspaceID+"/"+authoredStaleTaskID {
		t.Fatalf("expected stale authored task detail to return one active cluster, got %+v", staleDetail.Clusters)
	}

	inactiveDetail, err := store.BuildCorridorAuthorityTaskDetail(ctx, workspaceID, authoredFreshTaskID)
	if err != nil {
		t.Fatalf("build corridor authority task detail for inactive authored task: %v", err)
	}
	requireSameWorkspaceTimeAuthorityFieldsExternal(t, inactiveDetail.TimeAuthority, authority)
	if inactiveDetail.Task.TaskID != authoredFreshTaskID || len(inactiveDetail.Clusters) != 0 {
		t.Fatalf("expected inactive authored task detail to keep empty cluster linkage, got %+v", inactiveDetail)
	}
}

func requireSameWorkspaceTimeAuthorityFieldsExternal(t *testing.T, got, want sqlite.WorkspaceTimeAuthority) {
	t.Helper()

	if got.WorkspaceID != want.WorkspaceID || got.CurrentEpoch != want.CurrentEpoch || got.PolicyMode != want.PolicyMode || got.EpochAnchorAt != want.EpochAnchorAt || got.RuntimeEventAnchorAt != want.RuntimeEventAnchorAt || got.ReferenceAt == "" {
		t.Fatalf("unexpected workspace time authority fields: got=%+v want=%+v", got, want)
	}
	requireWorkspaceTimeAuthorityTemporalContractExternal(t, got)
	requireWorkspaceTimeAuthorityTemporalContractExternal(t, want)
}

func requireWorkspaceTimeAuthorityTemporalContractExternal(t *testing.T, authority sqlite.WorkspaceTimeAuthority) {
	t.Helper()
	if authority.TemporalContract == nil {
		t.Fatalf("expected workspace time authority temporal contract, got %+v", authority)
	}
	contract := authority.TemporalContract
	if contract.SchemaVersion != "1.0" ||
		contract.Domain != "control_epoch" ||
		contract.HorizonKind != "current_epoch" ||
		contract.Basis != "control_epoch" ||
		contract.Mapping != "explicit_phi_required" ||
		contract.WallClockComparable ||
		contract.State != "LIVE" {
		t.Fatalf("unexpected workspace time authority temporal contract %+v", contract)
	}
	if contract.CurrentEpoch != authority.CurrentEpoch || contract.TargetEpoch != authority.CurrentEpoch || contract.ReferenceAt != authority.ReferenceAt {
		t.Fatalf("expected workspace time authority temporal contract to mirror current epoch/reference_at, got contract=%+v authority=%+v", contract, authority)
	}
}
