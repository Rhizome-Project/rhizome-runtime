package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestConfirmTensionRejectsMissingWorkspaceAuthorityWithoutLifecycleSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-confirm-missing-authority", "task-d2a-tension-confirm")

	beforeDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
	beforeConfirmedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, scenario.workspaceID, "workspace"); err != nil {
		t.Fatalf("remove auto-seeded workspace authority: %v", err)
	}

	_, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "missing authority should fail closed",
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing workspace authority reject, got %+v", reject)
	}

	afterDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected missing authority reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeConfirmedEvents {
		t.Fatalf("expected no new tension.confirmed events after missing-authority reject, before=%d after=%d", beforeConfirmedEvents, got)
	}
	if got := countAuthorityRejectEvents(t, ctx, store, scenario.workspaceID); got != beforeRejects {
		t.Fatalf("expected missing-authority reject not to fabricate authority.rejected evidence, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestResolveTensionRejectsStaleWorkspaceAuthorityWithoutLifecycleSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-resolve-stale-authority", "task-d2a-tension-resolve")
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	beforeDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, scenario.workspaceID)
	beforeResolvedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.resolved",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, scenario.workspaceID, current, "authnode-999-3401")

	_, err := store.ResolveTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "stale authority should fail closed",
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale workspace authority reject, got %+v", reject)
	}

	afterDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected stale authority reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.resolved",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeResolvedEvents {
		t.Fatalf("expected no new tension.resolved events after stale-authority reject, before=%d after=%d", beforeResolvedEvents, got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, scenario.workspaceID, string(sqlite.AuthorityRejectStale))
	if got := countAuthorityRejectEvents(t, ctx, store, scenario.workspaceID); got != beforeRejects+1 {
		t.Fatalf("expected stale-authority reject to journal authority.rejected once, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, scenario.workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestResolveTensionCascadeRuntimeEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-cascade-authority-metadata", "task-d2a-tension-cascade")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)
	child := seedD2ATensionGapChild(t, ctx, store, scenario)

	if err := store.AddTensionDependency(ctx, scenario.workspaceID, child.TensionID, primary.TensionID, "SUBSUMED_BY"); err != nil {
		t.Fatalf("add tension dependency: %v", err)
	}

	resolved, err := store.ResolveTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "resolve parent with cascaded child event",
	})
	if err != nil {
		t.Fatalf("resolve tension: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, resolved.Event, authority)

	childEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.resolved",
		EntityType:  "tension",
		EntityID:    child.TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list child tension runtime events: %v", err)
	}
	if len(childEvents) == 0 {
		t.Fatal("expected cascaded child tension runtime event")
	}
	assertRuntimeEventAuthorityMetadata(t, childEvents[0], authority)
	if payload := decodeRuntimeEventPayload(t, childEvents[0].PayloadJSON); payload["cascaded_from"] != primary.TensionID {
		t.Fatalf("expected cascaded child payload to reference parent tension %q, got %+v", primary.TensionID, payload)
	}
}

func TestRefreshTensionsRuntimeEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-tension-refresh-authority-metadata", "task-d2a-tension-refresh")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if len(refresh.Events) == 0 {
		t.Fatalf("expected refresh to emit runtime events, got %+v", refresh)
	}

	assertRuntimeEventAuthorityMetadata(t, refresh.Events[0], authority)

	persisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   refresh.Events[0].EventType,
		EntityType:  "tension",
		EntityID:    refresh.Events[0].EntityID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list persisted refresh runtime events: %v", err)
	}
	if len(persisted) == 0 {
		t.Fatalf("expected persisted refresh runtime event for %+v", refresh.Events[0])
	}
	assertRuntimeEventAuthorityMetadata(t, persisted[0], authority)
}

func TestRefreshTensionsRuntimeEventsCarryPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-tension-refresh-prompt-context", "task-d2a-tension-refresh-prompt")
	const operatorID = "operator-tension-refresh"

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID:                scenario.workspaceID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.refresh", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.refresh",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("refresh tensions with prompt context: %v", err)
	}
	if len(refresh.Events) == 0 {
		t.Fatalf("expected refresh to emit runtime events, got %+v", refresh)
	}

	event := refresh.Events[0]
	assertWorkspaceTensionPromptContextEnvelope(t, event.PayloadJSON, map[string]string{
		"surface":        "workspace.tension.refresh",
		"workspace_id":   scenario.workspaceID,
		"principal_type": "human",
		"principal_id":   operatorID,
		"tension_id":     event.EntityID,
		"event_kind":     event.EventType,
		"actor_type":     "system",
		"actor_id":       operatorID,
	})
}

func TestConfirmTensionRuntimeEventCarriesPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-confirm-prompt-context", "task-d2a-tension-confirm-prompt")
	const operatorID = "operator-tension-confirm"

	confirmed, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		ActorID:                    operatorID,
		Reason:                     "confirm with prompt context",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.confirm", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.confirm",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("confirm tension with prompt context: %v", err)
	}
	assertWorkspaceTensionPromptContextEnvelope(t, confirmed.Event.PayloadJSON, map[string]string{
		"surface":         "workspace.tension.confirm",
		"workspace_id":    scenario.workspaceID,
		"principal_type":  "human",
		"principal_id":    operatorID,
		"tension_id":      primary.TensionID,
		"event_kind":      "tension.confirmed",
		"actor_type":      "operator",
		"actor_id":        operatorID,
		"lifecycle_state": "ACTIVE",
		"review_status":   "CONFIRMED",
	})
}

func TestTensionPromptContextRejectsForgedPrincipalWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-forged-prompt-context", "task-d2a-tension-forged-prompt")
	beforeDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeConfirmedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	_, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		ActorID:                    "operator-a",
		Reason:                     "forged prompt context should fail closed",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.confirm", "server_rpc", scenario.workspaceID, "human", "operator-b"),
		PromptContextSurface:       "workspace.tension.confirm",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected forged tension prompt context to fail closed")
	}

	afterDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected forged prompt context reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeConfirmedEvents {
		t.Fatalf("expected no new tension.confirmed event after forged prompt context, before=%d after=%d", beforeConfirmedEvents, got)
	}
}

func TestTensionPromptContextRejectsOperationSurfaceMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-surface-mismatch", "task-d2a-tension-surface-mismatch")
	beforeDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeConfirmedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	_, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		ActorID:                    "operator-a",
		Reason:                     "surface mismatch should fail closed",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.refresh", "server_rpc", scenario.workspaceID, "human", "operator-a"),
		PromptContextSurface:       "workspace.tension.refresh",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected confirm with refresh prompt context surface to fail closed")
	}

	afterDetail := mustTensionDetailForAuthorityReject(t, ctx, store, scenario.workspaceID, primary.TensionID)
	if afterDetail.Tension.LifecycleState != beforeDetail.Tension.LifecycleState ||
		afterDetail.Tension.ReviewStatus != beforeDetail.Tension.ReviewStatus ||
		afterDetail.Tension.UpdatedAt != beforeDetail.Tension.UpdatedAt {
		t.Fatalf("expected surface mismatch reject not to mutate tension, before=%+v after=%+v", beforeDetail.Tension, afterDetail.Tension)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.confirmed",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeConfirmedEvents {
		t.Fatalf("expected no new tension.confirmed event after surface mismatch, before=%d after=%d", beforeConfirmedEvents, got)
	}
}

func TestRefreshTensionsPromptContextRejectsOperationSurfaceMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-tension-refresh-surface-mismatch", "task-d2a-tension-refresh-surface-mismatch")
	beforeDetectedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.detected",
		EntityType:  "tension",
		Limit:       50,
	})
	beforeRefreshedEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.refreshed",
		EntityType:  "workspace_tension_refresh",
		Limit:       50,
	})

	_, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID:                scenario.workspaceID,
		ActorID:                    "operator-a",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.lifecycle.update", "server_rpc", scenario.workspaceID, "human", "operator-a"),
		PromptContextSurface:       "workspace.tension.lifecycle.update",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected refresh with lifecycle-update prompt context surface to fail closed")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.detected",
		EntityType:  "tension",
		Limit:       50,
	}); got != beforeDetectedEvents {
		t.Fatalf("expected no tension.detected events after refresh surface mismatch, before=%d after=%d", beforeDetectedEvents, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.refreshed",
		EntityType:  "workspace_tension_refresh",
		Limit:       50,
	}); got != beforeRefreshedEvents {
		t.Fatalf("expected no tension.refreshed summary event after refresh surface mismatch, before=%d after=%d", beforeRefreshedEvents, got)
	}
}

func TestTensionDependencyRuntimeEventsCarryPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-dependency-prompt-context", "task-d2a-tension-dependency-prompt")
	child := seedD2ATensionGapChild(t, ctx, store, scenario)
	const operatorID = "operator-tension-dependency"

	added, err := store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         primary.TensionID,
		DependencyType:             "SUBSUMED_BY",
		ActorID:                    operatorID,
		Reason:                     "link child to parent",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.add.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.add.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("add tension dependency with prompt context: %v", err)
	}
	if !added.Changed {
		t.Fatalf("expected dependency add to change graph, got %+v", added)
	}
	assertWorkspaceTensionPromptContextEnvelope(t, added.Event.PayloadJSON, map[string]string{
		"surface":               "workspace.tension.add.dependency",
		"workspace_id":          scenario.workspaceID,
		"principal_type":        "human",
		"principal_id":          operatorID,
		"tension_id":            child.TensionID,
		"depends_on_tension_id": primary.TensionID,
		"dependency_type":       "SUBSUMED_BY",
		"event_kind":            "tension.dependency.added",
		"actor_type":            "operator",
		"actor_id":              operatorID,
	})
	if got := countTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, primary.TensionID); got != 1 {
		t.Fatalf("expected one dependency edge after add, got %d", got)
	}
	beforeDuplicateEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.added",
		EntityType:  "tension",
		EntityID:    child.TensionID,
		Limit:       10,
	})
	duplicate, err := store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         primary.TensionID,
		DependencyType:             "SUBSUMED_BY",
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.add.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.add.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("duplicate add tension dependency: %v", err)
	}
	if duplicate.Changed {
		t.Fatalf("expected duplicate dependency add to be an explicit no-op, got %+v", duplicate)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.added",
		EntityType:  "tension",
		EntityID:    child.TensionID,
		Limit:       10,
	}); got != beforeDuplicateEvents {
		t.Fatalf("expected duplicate dependency add not to emit another event, before=%d after=%d", beforeDuplicateEvents, got)
	}

	removed, err := store.RemoveTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         primary.TensionID,
		ActorID:                    operatorID,
		Reason:                     "unlink child from parent",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.remove.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.remove.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("remove tension dependency with prompt context: %v", err)
	}
	if !removed.Changed {
		t.Fatalf("expected dependency remove to change graph, got %+v", removed)
	}
	assertWorkspaceTensionPromptContextEnvelope(t, removed.Event.PayloadJSON, map[string]string{
		"surface":               "workspace.tension.remove.dependency",
		"workspace_id":          scenario.workspaceID,
		"principal_type":        "human",
		"principal_id":          operatorID,
		"tension_id":            child.TensionID,
		"depends_on_tension_id": primary.TensionID,
		"dependency_type":       "SUBSUMED_BY",
		"event_kind":            "tension.dependency.removed",
		"actor_type":            "operator",
		"actor_id":              operatorID,
	})
	if got := countTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, primary.TensionID); got != 0 {
		t.Fatalf("expected dependency edge removed, got %d", got)
	}
}

func TestTensionDependencyRejectsSelfAndCrossWorkspaceEdgesWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-dependency-invariants", "task-d2a-tension-dependency-invariants")
	child := seedD2ATensionGapChild(t, ctx, store, scenario)
	const otherWorkspaceID = "ws-d2a-tension-dependency-invariants-other"
	const otherTensionID = "tension-dependency-other-workspace"
	const operatorID = "operator-tension-dependency-invariants"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: otherWorkspaceID,
		Title:       "Other dependency workspace",
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

	_, err := store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		DependsOnTensionID:         primary.TensionID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.add.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.add.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err == nil {
		t.Fatal("expected self-dependency to fail closed")
	}
	if got := countTensionDependencyEdges(t, ctx, store, scenario.workspaceID, primary.TensionID, primary.TensionID); got != 0 {
		t.Fatalf("expected no self-dependency edge, got %d", got)
	}

	_, err = store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         otherTensionID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.add.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.add.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err == nil {
		t.Fatal("expected cross-workspace dependency to fail closed")
	}
	if got := countTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, otherTensionID); got != 0 {
		t.Fatalf("expected no cross-workspace dependency edge, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.added",
		EntityType:  "tension",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no dependency added events after rejected edges, got %d", got)
	}
	_, err = store.RemoveTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         otherTensionID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.remove.dependency", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.remove.dependency",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err == nil {
		t.Fatal("expected cross-workspace dependency remove to fail closed")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.removed",
		EntityType:  "tension",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no dependency removed events after rejected remove, got %d", got)
	}
	if otherWorkspaceID == scenario.workspaceID {
		t.Fatal("expected cross-workspace fixture to use a distinct workspace")
	}
}

func TestTensionDependencyPromptContextRejectsOperationSurfaceMismatchWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-dependency-surface-mismatch", "task-d2a-tension-dependency-surface-mismatch")
	child := seedD2ATensionGapChild(t, ctx, store, scenario)
	beforeEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.added",
		EntityType:  "tension",
		EntityID:    child.TensionID,
		Limit:       10,
	})

	_, err := store.AddTensionDependencyWithContext(ctx, sqlite.TensionDependencyMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  child.TensionID,
		DependsOnTensionID:         primary.TensionID,
		DependencyType:             "SUBSUMED_BY",
		ActorID:                    "operator-a",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.confirm", "server_rpc", scenario.workspaceID, "human", "operator-a"),
		PromptContextSurface:       "workspace.tension.confirm",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-a",
	})
	if err == nil {
		t.Fatal("expected dependency add with confirm prompt context surface to fail closed")
	}
	if got := countTensionDependencyEdges(t, ctx, store, scenario.workspaceID, child.TensionID, primary.TensionID); got != 0 {
		t.Fatalf("expected no dependency edge after surface mismatch, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.dependency.added",
		EntityType:  "tension",
		EntityID:    child.TensionID,
		Limit:       10,
	}); got != beforeEvents {
		t.Fatalf("expected no dependency runtime event after surface mismatch, before=%d after=%d", beforeEvents, got)
	}
}

func TestTensionCoalitionAgentAttachDetachRuntimeEventsCarryPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-member-prompt-context", "task-d2a-tension-coalition-member")
	const operatorID = "operator-tension-coalition-member"
	const agentID = "agent-a"

	attached, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    agentID,
		ActorID:                    operatorID,
		SuccessCriterion:           "stabilize coalition prompt context",
		Reason:                     "attach agent to tension coalition",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("attach tension agent with prompt context: %v", err)
	}
	if !attached.Changed || attached.Event.EventType != "tension.agent.attached" {
		t.Fatalf("expected changed tension.agent.attached event, got %+v", attached)
	}
	if attached.Coalition.CoalitionID == "" {
		t.Fatalf("expected attached coalition id, got %+v", attached)
	}
	assertWorkspaceTensionPromptContextEnvelope(t, attached.Event.PayloadJSON, map[string]string{
		"surface":                "workspace.tension.agent.attach",
		"workspace_id":           scenario.workspaceID,
		"principal_type":         "human",
		"principal_id":           operatorID,
		"tension_id":             primary.TensionID,
		"event_kind":             "tension.agent.attached",
		"actor_type":             "operator",
		"actor_id":               operatorID,
		"coalition_id":           attached.Coalition.CoalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "attached",
		"coalition_member_count": "1",
		"coalition_status":       "FORMING",
	})
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, agentID); got != 1 {
		t.Fatalf("expected attached coalition member, got %d", got)
	}

	beforeDuplicateEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	duplicate, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    agentID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("duplicate attach tension agent: %v", err)
	}
	if duplicate.Changed {
		t.Fatalf("expected duplicate attach to be no-op, got %+v", duplicate)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeDuplicateEvents {
		t.Fatalf("expected duplicate attach not to emit another event, before=%d after=%d", beforeDuplicateEvents, got)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE workspace_coalition_members SET min_stay_until_epoch = 0 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, scenario.workspaceID, attached.Coalition.CoalitionID, agentID); err != nil {
		t.Fatalf("relax coalition minimum tenure: %v", err)
	}
	detached, err := store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    agentID,
		ActorID:                    operatorID,
		Reason:                     "detach agent from tension coalition",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.detach", "server_rpc", scenario.workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.agent.detach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("detach tension agent with prompt context: %v", err)
	}
	if !detached.Changed || detached.Event.EventType != "tension.agent.detached" {
		t.Fatalf("expected changed tension.agent.detached event, got %+v", detached)
	}
	assertWorkspaceTensionPromptContextEnvelope(t, detached.Event.PayloadJSON, map[string]string{
		"surface":                "workspace.tension.agent.detach",
		"workspace_id":           scenario.workspaceID,
		"principal_type":         "human",
		"principal_id":           operatorID,
		"tension_id":             primary.TensionID,
		"event_kind":             "tension.agent.detached",
		"actor_type":             "operator",
		"actor_id":               operatorID,
		"coalition_id":           attached.Coalition.CoalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "detached",
		"coalition_member_count": "0",
		"coalition_status":       "DISBANDED",
	})
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, agentID); got != 0 {
		t.Fatalf("expected detached coalition member to be removed, got %d", got)
	}
}

func TestTensionCoalitionAgentAttachRejectsSurfaceMismatchWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-member-surface-mismatch", "task-d2a-tension-coalition-member-surface")
	beforeCoalitions := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID)
	beforeEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	_, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-surface-mismatch",
		ActorID:                    "operator-surface-mismatch",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.confirm", "server_rpc", scenario.workspaceID, "human", "operator-surface-mismatch"),
		PromptContextSurface:       "workspace.tension.confirm",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-surface-mismatch",
	})
	if err == nil {
		t.Fatal("expected tension agent attach with confirm prompt context surface to fail closed")
	}
	if got := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID); got != beforeCoalitions {
		t.Fatalf("expected no coalition side effect after surface mismatch, before=%d after=%d", beforeCoalitions, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeEvents {
		t.Fatalf("expected no attach runtime event after surface mismatch, before=%d after=%d", beforeEvents, got)
	}
}

func TestTensionCoalitionAgentDetachRejectsMissingTargetWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, _ := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-member-missing-target", "task-d2a-tension-coalition-member-missing")

	_, err := store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                "missing-coalition",
		AgentID:                    "agent-missing-target",
		ActorID:                    "operator-missing-target",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.detach", "server_rpc", scenario.workspaceID, "human", "operator-missing-target"),
		PromptContextSurface:       "workspace.tension.agent.detach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-missing-target",
	})
	if err == nil {
		t.Fatal("expected missing coalition detach target to fail closed")
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no detach runtime event after missing coalition, got %d", got)
	}
}

func TestTensionCoalitionAgentRejectsPrincipalActorAndAgentSpoofingWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-member-principal-spoof", "task-d2a-tension-coalition-member-principal")
	beforeCoalitions := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID)

	_, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-a",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", "operator-b"),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-b",
	})
	if err == nil {
		t.Fatal("expected attach principal/actor mismatch to fail closed")
	}
	_, err = store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-a",
		ActorID:                    "agent-b",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "agent", "agent-b"),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   "agent-b",
	})
	if err == nil {
		t.Fatal("expected attach agent-principal target spoof to fail closed")
	}
	_, err = store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-agent-tool-surface",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("agent.tension.attach", "agent_tool", scenario.workspaceID, "human", "operator-agent-tool-surface"),
		PromptContextSurface:       "agent.tension.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-agent-tool-surface",
	})
	if err == nil {
		t.Fatal("expected attach human principal with agent-tool surface to fail closed")
	}
	if got := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID); got != beforeCoalitions {
		t.Fatalf("expected no coalition side effect after attach spoofing, before=%d after=%d", beforeCoalitions, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no attach runtime event after spoofing, got %d", got)
	}

	attached, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-valid",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", "operator-valid"),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-valid",
	})
	if err != nil {
		t.Fatalf("seed valid attach before detach spoofing: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE workspace_coalition_members SET min_stay_until_epoch = 0 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, scenario.workspaceID, attached.Coalition.CoalitionID, "agent-a"); err != nil {
		t.Fatalf("relax coalition minimum tenure: %v", err)
	}
	beforeDetachEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	_, err = store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-a",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.detach", "server_rpc", scenario.workspaceID, "human", "operator-b"),
		PromptContextSurface:       "workspace.tension.agent.detach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-b",
	})
	if err == nil {
		t.Fatal("expected detach principal/actor mismatch to fail closed")
	}
	_, err = store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-a",
		ActorID:                    "agent-b",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.detach", "server_rpc", scenario.workspaceID, "agent", "agent-b"),
		PromptContextSurface:       "workspace.tension.agent.detach",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   "agent-b",
	})
	if err == nil {
		t.Fatal("expected detach agent-principal target spoof to fail closed")
	}
	_, err = store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-agent-tool-surface",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("agent.tension.detach", "agent_tool", scenario.workspaceID, "human", "operator-agent-tool-surface"),
		PromptContextSurface:       "agent.tension.detach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-agent-tool-surface",
	})
	if err == nil {
		t.Fatal("expected detach human principal with agent-tool surface to fail closed")
	}
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, "agent-a"); got != 1 {
		t.Fatalf("expected spoofed detach to keep member row, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeDetachEvents {
		t.Fatalf("expected no detach runtime event after spoofing, before=%d after=%d", beforeDetachEvents, got)
	}
}

func TestTensionCoalitionAgentCoalitionSurfacesEnforceActorMembershipAtStorageBoundary(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-surface-membership", "task-d2a-tension-coalition-surface-membership")

	attached, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-a",
		ActorID:                    "operator-seed-coalition",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", "operator-seed-coalition"),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-seed-coalition",
	})
	if err != nil {
		t.Fatalf("seed coalition member: %v", err)
	}
	beforeAttachEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})

	_, err = store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-c",
		ActorID:                    "agent-b",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.invite", "server_rpc", scenario.workspaceID, "agent", "agent-b"),
		PromptContextSurface:       "coalition.invite",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   "agent-b",
	})
	if !errors.Is(err, sqlite.ErrCoalitionActorNotMember) {
		t.Fatalf("expected direct coalition.invite by non-member actor to fail closed, got %v", err)
	}
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, "agent-c"); got != 0 {
		t.Fatalf("expected forged invite not to attach target, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeAttachEvents {
		t.Fatalf("expected forged invite not to emit attach event, before=%d after=%d", beforeAttachEvents, got)
	}

	beforeDetachEvents := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	})
	_, err = store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-a",
		ActorID:                    "agent-b",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.kick", "server_rpc", scenario.workspaceID, "agent", "agent-b"),
		PromptContextSurface:       "coalition.kick",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   "agent-b",
	})
	if !errors.Is(err, sqlite.ErrCoalitionActorNotMember) {
		t.Fatalf("expected direct coalition.kick by non-member actor to fail closed, got %v", err)
	}
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, "agent-a"); got != 1 {
		t.Fatalf("expected forged kick not to remove target, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeDetachEvents {
		t.Fatalf("expected forged kick not to emit detach event, before=%d after=%d", beforeDetachEvents, got)
	}

	_, err = store.DetachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		CoalitionID:                attached.Coalition.CoalitionID,
		AgentID:                    "agent-a",
		ActorID:                    "agent-a",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("coalition.kick", "server_rpc", scenario.workspaceID, "agent", "agent-a"),
		PromptContextSurface:       "coalition.kick",
		PromptContextPrincipalType: "agent",
		PromptContextPrincipalID:   "agent-a",
	})
	if !errors.Is(err, sqlite.ErrCoalitionSelfKick) {
		t.Fatalf("expected direct coalition.kick self-removal to fail closed as self-kick, got %v", err)
	}
	if got := countTensionCoalitionMembers(t, ctx, store, scenario.workspaceID, attached.Coalition.CoalitionID, "agent-a"); got != 1 {
		t.Fatalf("expected self-kick not to remove target, got %d", got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.detached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != beforeDetachEvents {
		t.Fatalf("expected self-kick not to emit detach event, before=%d after=%d", beforeDetachEvents, got)
	}
}

func TestTensionCoalitionAgentAttachRejectsUnknownAgentWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario, primary := seedD2ATensionAuthorityScenario(t, ctx, store, "ws-d2a-tension-coalition-member-unknown-agent", "task-d2a-tension-coalition-member-unknown")
	beforeCoalitions := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID)

	_, err := store.AttachTensionAgentWithContext(ctx, sqlite.TensionCoalitionMemberMutationInput{
		WorkspaceID:                scenario.workspaceID,
		TensionID:                  primary.TensionID,
		AgentID:                    "agent-unknown",
		ActorID:                    "operator-unknown-agent",
		PromptContextEnvelope:      sqlite.BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.agent.attach", "server_rpc", scenario.workspaceID, "human", "operator-unknown-agent"),
		PromptContextSurface:       "workspace.tension.agent.attach",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-unknown-agent",
	})
	if err == nil {
		t.Fatal("expected unknown agent attach to fail closed")
	}
	if got := countTensionCoalitions(t, ctx, store, scenario.workspaceID, primary.TensionID); got != beforeCoalitions {
		t.Fatalf("expected unknown-agent attach not to leave an empty coalition, before=%d after=%d", beforeCoalitions, got)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    primary.TensionID,
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no attach runtime event after unknown agent, got %d", got)
	}
}

func TestEnsureGovernedTensionRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-governed-tension-missing-authority", "task-d2a-governed-missing-authority")
	workspaceID := scenario.workspaceID
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	beforeUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace"); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}

	err := store.EnsureGovernedTension(ctx, sqlite.EnsureGovernedTensionInput{
		WorkspaceID:    workspaceID,
		TensionType:    "failure",
		ProtoClusterID: "cluster-a",
		AnchorRef:      "entity-a",
		Title:          "Governed missing authority",
		Summary:        "Missing authority should fail closed",
		EvidenceRefs:   []string{"evt-missing-authority"},
	})
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", reject)
	}

	items, listErr := store.ListTensions(ctx, sqlite.TensionFilter{WorkspaceID: workspaceID, Limit: 20})
	if listErr != nil {
		t.Fatalf("list tensions after missing-authority reject: %v", listErr)
	}
	if len(items) != 0 {
		t.Fatalf("expected no governed tension side effects after missing-authority reject, got %+v", items)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.emerged",
		EntityType:  "tension",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no tension.emerged events after missing-authority reject, got %d", got)
	}
	if got := countAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects {
		t.Fatalf("expected missing-authority reject not to fabricate authority.rejected evidence, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestEnsureGovernedTensionRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-governed-tension-stale-authority", "task-d2a-governed-stale-authority")
	workspaceID := scenario.workspaceID
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	beforeUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	beforeRejects := countAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-3411")

	err := store.EnsureGovernedTension(ctx, sqlite.EnsureGovernedTensionInput{
		WorkspaceID:    workspaceID,
		TensionType:    "failure",
		ProtoClusterID: "cluster-b",
		AnchorRef:      "entity-b",
		Title:          "Governed stale authority",
		Summary:        "Stale authority should fail closed",
		EvidenceRefs:   []string{"evt-stale-authority"},
	})
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		t.Fatalf("expected authority reject, got %v", err)
	}
	if reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", reject)
	}

	items, listErr := store.ListTensions(ctx, sqlite.TensionFilter{WorkspaceID: workspaceID, Limit: 20})
	if listErr != nil {
		t.Fatalf("list tensions after stale-authority reject: %v", listErr)
	}
	if len(items) != 0 {
		t.Fatalf("expected no governed tension side effects after stale-authority reject, got %+v", items)
	}
	if got := countRuntimeEvents(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.emerged",
		EntityType:  "tension",
		Limit:       10,
	}); got != 0 {
		t.Fatalf("expected no tension.emerged events after stale-authority reject, got %d", got)
	}
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if got := countAuthorityRejectEvents(t, ctx, store, workspaceID); got != beforeRejects+1 {
		t.Fatalf("expected stale-authority reject to journal authority.rejected once, before=%d after=%d", beforeRejects, got)
	}
	if afterUpdatedAt := mustTensionAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestEnsureGovernedTensionRuntimeEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-governed-tension-authority-metadata", "task-d2a-governed-authority-metadata")
	workspaceID := scenario.workspaceID
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	first := sqlite.EnsureGovernedTensionInput{
		WorkspaceID:    workspaceID,
		TensionType:    "failure",
		ProtoClusterID: "cluster-c",
		AnchorRef:      "entity-c",
		Title:          "Governed tension metadata",
		Summary:        "First hit",
		EvidenceRefs:   []string{"evt-c-1"},
	}
	if err := store.EnsureGovernedTension(ctx, first); err != nil {
		t.Fatalf("ensure governed tension create: %v", err)
	}

	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		TensionType: "failure",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list governed tensions: %v", err)
	}
	record := requireTensionByTypeForAuthority(t, items, "failure")

	emerged, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.emerged",
		EntityType:  "tension",
		EntityID:    record.TensionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list tension.emerged events: %v", err)
	}
	if len(emerged) == 0 {
		t.Fatalf("expected tension.emerged event for %s", record.TensionID)
	}
	assertRuntimeEventAuthorityMetadata(t, emerged[0], authority)

	second := first
	second.Summary = "Second hit"
	second.EvidenceRefs = []string{"evt-c-2"}
	if err := store.EnsureGovernedTension(ctx, second); err != nil {
		t.Fatalf("ensure governed tension refresh: %v", err)
	}

	refreshed, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.refreshed",
		EntityType:  "tension",
		EntityID:    record.TensionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list tension.refreshed events: %v", err)
	}
	if len(refreshed) == 0 {
		t.Fatalf("expected tension.refreshed event for %s", record.TensionID)
	}
	assertRuntimeEventAuthorityMetadata(t, refreshed[0], authority)
}

func seedD2ATensionAuthorityScenario(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) (instrumentationScenario, sqlite.TensionRecord) {
	t.Helper()

	scenario := seedInstrumentationScenario(t, ctx, store, workspaceID, taskID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      scenario.runbookDocKey,
		Title:       "Runbook",
		Content:     "Instrumentation authority scenario runbook v2",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert workspace doc for tension authority scenario: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:  "artifact-" + workspaceID + "-gap",
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Gap Evidence",
		ArtifactRef: "artifact://" + workspaceID + "/gap",
		Kind:        "note",
		ContentType: "text/plain",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace artifact for tension authority scenario: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	return scenario, requireTensionByTypeForAuthority(t, items, "bottleneck")
}

func seedD2ATensionGapChild(t *testing.T, ctx context.Context, store *sqlite.Store, scenario instrumentationScenario) sqlite.TensionRecord {
	t.Helper()

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: scenario.workspaceID,
		DocKey:      "gap-followup",
		Title:       "Gap Followup",
		Content:     "Escalation followup doc to force a second tension candidate",
		UpdatedBy:   "tests",
	}); err != nil {
		t.Fatalf("upsert gap followup doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:  "artifact-" + scenario.workspaceID + "-gap-followup",
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Title:       "Gap Followup Artifact",
		ArtifactRef: "artifact://" + scenario.workspaceID + "/gap-followup",
		Kind:        "note",
		ContentType: "text/plain",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create gap followup artifact: %v", err)
	}
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "dashboard",
	}); err != nil {
		t.Fatalf("refresh tensions for gap child: %v", err)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions after gap child seed: %v", err)
	}
	return requireTensionByTypeForAuthority(t, items, "gap")
}

func requireTensionByTypeForAuthority(t *testing.T, items []sqlite.TensionRecord, wantType string) sqlite.TensionRecord {
	t.Helper()

	for _, item := range items {
		if item.TensionType == wantType {
			return item
		}
	}
	t.Fatalf("expected tension type %q, got %+v", wantType, items)
	return sqlite.TensionRecord{}
}

func mustTensionDetailForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID string) sqlite.TensionDetail {
	t.Helper()

	detail, err := store.GetTension(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension detail %s/%s: %v", workspaceID, tensionID, err)
	}
	return detail
}

func mustTensionAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at for %s: %v", workspaceID, err)
	}
	return updatedAt
}

func decodeRuntimeEventPayload(t *testing.T, raw string) map[string]any {
	t.Helper()

	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode runtime event payload: %v", err)
	}
	return out
}

func assertWorkspaceTensionPromptContextEnvelope(t *testing.T, payloadJSON string, expected map[string]string) {
	t.Helper()

	payload := decodeRuntimeEventPayload(t, payloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace tension prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	base := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_tension_write",
		"origin":                             "server_rpc",
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range expected {
		base[key] = value
	}
	for key, want := range base {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("workspace tension prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace tension prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func countTensionDependencyEdges(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, dependsOnTensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tension_dependencies WHERE workspace_id = ? AND tension_id = ? AND depends_on_tension_id = ?`, workspaceID, tensionID, dependsOnTensionID).Scan(&count); err != nil {
		t.Fatalf("count tension dependency edges: %v", err)
	}
	return count
}

func countTensionCoalitionMembers(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, coalitionID, agentID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_coalition_members WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`, workspaceID, coalitionID, agentID).Scan(&count); err != nil {
		t.Fatalf("count tension coalition members: %v", err)
	}
	return count
}

func countTensionCoalitions(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ?`, workspaceID, tensionID).Scan(&count); err != nil {
		t.Fatalf("count tension coalitions: %v", err)
	}
	return count
}
