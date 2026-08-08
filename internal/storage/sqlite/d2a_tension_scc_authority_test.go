package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRefreshMetaTensionsRejectsMissingWorkspaceAuthorityWithoutMetaSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-authority-missing"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, authorityScopeWorkspace); err != nil {
		t.Fatalf("delete workspace authority: %v", err)
	}
	beforeRejects := countMetaTensionAuthorityRejectEvents(t, ctx, store, workspaceID)

	err := store.RefreshMetaTensions(ctx, workspaceID)
	if err == nil {
		t.Fatal("expected missing workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %v", err)
	}
	assertNoMetaTensionRuntimeEvents(t, ctx, store, workspaceID)
	assertNoMetaTensionRows(t, ctx, store, workspaceID)
	if afterRejects := countMetaTensionAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects {
		t.Fatalf("expected missing-authority RefreshMetaTensions to avoid authority.rejected journaling, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func TestRefreshMetaTensionsRejectsStaleWorkspaceAuthorityWithoutMetaSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-authority-stale"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	current := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	beforeRejects := countMetaTensionAuthorityRejectEvents(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-2951")

	err := store.RefreshMetaTensions(ctx, workspaceID)
	if err == nil {
		t.Fatal("expected stale workspace authority reject")
	}
	reject, ok := AsAuthorityReject(err)
	if !ok || reject == nil || reject.RejectCode != AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}
	assertNoMetaTensionRuntimeEvents(t, ctx, store, workspaceID)
	assertNoMetaTensionRows(t, ctx, store, workspaceID)
	if afterRejects := countMetaTensionAuthorityRejectEvents(t, ctx, store, workspaceID); afterRejects != beforeRejects+1 {
		t.Fatalf("expected stale-authority RefreshMetaTensions to journal one authority.rejected event, before=%d after=%d", beforeRejects, afterRejects)
	}
}

func TestRefreshMetaTensionsEmergentRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-authority-metadata"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if err := store.RefreshMetaTensions(ctx, workspaceID); err != nil {
		t.Fatalf("refresh meta tensions: %v", err)
	}

	tensions, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TensionType: "meta-tension",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list meta tensions: %v", err)
	}
	if len(tensions) != 1 {
		t.Fatalf("expected one meta tension, got %+v", tensions)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.emergent",
		EntityType:  "tension",
		EntityID:    tensions[0].TensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension.emergent runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one tension.emergent event for meta tension, got %+v", events)
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRefreshMetaTensionsWithContextRuntimeEventsCarryPromptContextEnvelope(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-condense-prompt-context"
	const operatorID = "operator-meta-condense"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "condense cyclic blockers",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("refresh meta tensions with context: %v", err)
	}
	if result.CreatedCount != 1 || result.DependencyAddedCount != 3 || result.ProcessedComponentCount != 1 {
		t.Fatalf("unexpected condense result: %+v", result)
	}
	if len(result.MetaTensionIDs) != 1 {
		t.Fatalf("expected one meta tension id, got %+v", result.MetaTensionIDs)
	}
	metaID := result.MetaTensionIDs[0]
	expectedCounts := map[string]int{
		"tension.emergent":         1,
		"tension.dependency.added": 3,
		"tension.condensed":        1,
	}
	actualCounts := map[string]int{}
	for _, event := range result.Events {
		actualCounts[event.EventType]++
		assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
			"surface":        "workspace.tension.condense",
			"workspace_id":   workspaceID,
			"principal_type": "human",
			"principal_id":   operatorID,
			"event_kind":     event.EventType,
			"actor_type":     "system",
			"actor_id":       operatorID,
		})
		if event.EventType == "tension.emergent" {
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                "workspace.tension.condense",
				"workspace_id":           workspaceID,
				"principal_type":         "human",
				"principal_id":           operatorID,
				"event_kind":             "tension.emergent",
				"actor_type":             "system",
				"actor_id":               operatorID,
				"tension_id":             metaID,
				"tension_type":           "meta-tension",
				"anchor_kind":            "scc_condensation",
				"scc_member_count":       "3",
				"condense_action":        "created",
				"scc_member_tension_ids": "T1,T2,T3",
			})
		}
		if event.EventType == "tension.dependency.added" {
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                "workspace.tension.condense",
				"workspace_id":           workspaceID,
				"principal_type":         "human",
				"principal_id":           operatorID,
				"event_kind":             "tension.dependency.added",
				"actor_type":             "system",
				"actor_id":               operatorID,
				"depends_on_tension_id":  metaID,
				"dependency_type":        "SUBSUMED_BY",
				"scc_member_count":       "3",
				"condense_action":        "dependency_linked",
				"scc_member_tension_ids": "T1,T2,T3",
			})
		}
		if event.EventType == "tension.condensed" {
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                   "workspace.tension.condense",
				"workspace_id":              workspaceID,
				"principal_type":            "human",
				"principal_id":              operatorID,
				"event_kind":                "tension.condensed",
				"actor_type":                "system",
				"actor_id":                  operatorID,
				"changed":                   "true",
				"processed_component_count": "1",
				"dependency_added_count":    "3",
			})
		}
	}
	for eventType, want := range expectedCounts {
		if actualCounts[eventType] != want {
			t.Fatalf("expected %d %s events, got counts %+v", want, eventType, actualCounts)
		}
	}
	if got := countMetaCondenseDependencyEdges(t, ctx, store, workspaceID, metaID); got != 3 {
		t.Fatalf("expected three SUBSUMED_BY member edges, got %d", got)
	}

	second, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "repeat condense should be explicit no-op",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("repeat refresh meta tensions with context: %v", err)
	}
	if second.Changed || second.CreatedCount != 0 || second.UpdatedCount != 0 || second.ResurrectedCount != 0 || second.DependencyAddedCount != 0 {
		t.Fatalf("expected repeat condensation to be an explicit no-op, got %+v", second)
	}
	if len(second.Events) != 1 || second.Events[0].EventType != "tension.condensed" {
		t.Fatalf("expected repeat condensation to emit only no-op summary event, got %+v", second.Events)
	}
	assertMetaCondensePromptContextEnvelope(t, second.Events[0].PayloadJSON, map[string]string{
		"surface":                   "workspace.tension.condense",
		"workspace_id":              workspaceID,
		"principal_type":            "human",
		"principal_id":              operatorID,
		"event_kind":                "tension.condensed",
		"actor_type":                "system",
		"actor_id":                  operatorID,
		"changed":                   "false",
		"processed_component_count": "1",
		"dependency_added_count":    "0",
	})
	if got := countMetaCondenseDependencyEdges(t, ctx, store, workspaceID, metaID); got != 3 {
		t.Fatalf("expected repeat condense not to duplicate SUBSUMED_BY edges, got %d", got)
	}
}

func TestRefreshMetaTensionsWithContextArchivesStaleMetaAfterCycleBreak(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-condense-stale-cleanup"
	const operatorID = "operator-meta-condense-cleanup"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	first, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "create meta before breaking cycle",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("initial condense: %v", err)
	}
	if len(first.MetaTensionIDs) != 1 {
		t.Fatalf("expected one initial meta tension, got %+v", first.MetaTensionIDs)
	}
	metaID := first.MetaTensionIDs[0]
	if got := countMetaCondenseDependencyEdges(t, ctx, store, workspaceID, metaID); got != 3 {
		t.Fatalf("expected three SUBSUMED_BY edges before cycle break, got %d", got)
	}

	if err := store.RemoveTensionDependency(ctx, workspaceID, "T3", "T1"); err != nil {
		t.Fatalf("break SCC cycle: %v", err)
	}

	cleanup, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "cleanup stale meta after cycle break",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("cleanup stale meta tensions: %v", err)
	}
	if !cleanup.Changed || cleanup.StaleMetaArchivedCount != 1 || cleanup.DependencyRemovedCount != 3 || cleanup.CreatedCount != 0 || cleanup.DependencyAddedCount != 0 {
		t.Fatalf("unexpected stale cleanup result: %+v", cleanup)
	}
	if got := countMetaCondenseDependencyEdges(t, ctx, store, workspaceID, metaID); got != 0 {
		t.Fatalf("expected stale SUBSUMED_BY edges removed after cycle break, got %d", got)
	}
	assertTensionLifecycleState(t, ctx, store, workspaceID, metaID, tensionLifecycleArchived)

	counts := map[string]int{}
	for _, event := range cleanup.Events {
		counts[event.EventType]++
		assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
			"surface":        "workspace.tension.condense",
			"workspace_id":   workspaceID,
			"principal_type": "human",
			"principal_id":   operatorID,
			"event_kind":     event.EventType,
			"actor_type":     "system",
			"actor_id":       operatorID,
		})
		switch event.EventType {
		case "tension.archived":
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                "workspace.tension.condense",
				"workspace_id":           workspaceID,
				"principal_type":         "human",
				"principal_id":           operatorID,
				"event_kind":             "tension.archived",
				"actor_type":             "system",
				"actor_id":               operatorID,
				"tension_id":             metaID,
				"tension_type":           "meta-tension",
				"lifecycle_state":        tensionLifecycleArchived,
				"condense_action":        "stale_meta_archived",
				"scc_member_count":       "3",
				"scc_member_tension_ids": "T1,T2,T3",
			})
		case "tension.dependency.removed":
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                "workspace.tension.condense",
				"workspace_id":           workspaceID,
				"principal_type":         "human",
				"principal_id":           operatorID,
				"event_kind":             "tension.dependency.removed",
				"actor_type":             "system",
				"actor_id":               operatorID,
				"depends_on_tension_id":  metaID,
				"dependency_type":        "SUBSUMED_BY",
				"condense_action":        "stale_dependency_removed",
				"scc_member_count":       "3",
				"scc_member_tension_ids": "T1,T2,T3",
			})
		case "tension.condensed":
			assertMetaCondensePromptContextEnvelope(t, event.PayloadJSON, map[string]string{
				"surface":                   "workspace.tension.condense",
				"workspace_id":              workspaceID,
				"principal_type":            "human",
				"principal_id":              operatorID,
				"event_kind":                "tension.condensed",
				"actor_type":                "system",
				"actor_id":                  operatorID,
				"changed":                   "true",
				"dependency_removed_count":  "3",
				"stale_meta_archived_count": "1",
				"stale_meta_tension_ids":    metaID,
			})
		}
	}
	if counts["tension.archived"] != 1 || counts["tension.dependency.removed"] != 3 || counts["tension.condensed"] != 1 {
		t.Fatalf("unexpected stale cleanup event counts: %+v", counts)
	}

	repeat, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "repeat stale cleanup should be no-op",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("repeat stale cleanup: %v", err)
	}
	if repeat.Changed || repeat.StaleMetaArchivedCount != 0 || repeat.DependencyRemovedCount != 0 || len(repeat.Events) != 1 || repeat.Events[0].EventType != "tension.condensed" {
		t.Fatalf("expected repeat stale cleanup to be a no-op summary only, got %+v", repeat)
	}
}

func TestRefreshMetaTensionsWithContextRejectsOrphanStaleMetaEdgeWithoutFalseGreen(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-condense-orphan-cleanup"
	const operatorID = "operator-meta-condense-orphan"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	first, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "create meta before orphan edge",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err != nil {
		t.Fatalf("initial condense: %v", err)
	}
	if len(first.MetaTensionIDs) != 1 {
		t.Fatalf("expected one initial meta tension, got %+v", first.MetaTensionIDs)
	}
	metaID := first.MetaTensionIDs[0]
	conn, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("open raw db connection for orphan fixture: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys for orphan fixture: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO workspace_tension_dependencies (workspace_id, tension_id, depends_on_tension_id, dependency_type, created_at) VALUES (?, ?, ?, 'SUBSUMED_BY', ?)`, workspaceID, "T-missing", metaID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert orphan stale meta edge: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore foreign keys after orphan fixture: %v", err)
	}
	if err := store.RemoveTensionDependency(ctx, workspaceID, "T3", "T1"); err != nil {
		t.Fatalf("break SCC cycle: %v", err)
	}
	beforeArchived := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.archived")
	beforeRemoved := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.dependency.removed")
	beforeCondensed := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.condensed")

	_, err = store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		Reason:                     "orphan cleanup should fail before false-green counts",
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err == nil {
		t.Fatal("expected orphan stale meta edge cleanup to fail closed")
	}
	if !strings.Contains(err.Error(), "cleanup requires durable removal evidence") {
		t.Fatalf("expected durable-removal-evidence error, got %v", err)
	}
	assertTensionLifecycleState(t, ctx, store, workspaceID, metaID, tensionLifecycleEmergent)
	if got := countMetaCondenseDependencyEdges(t, ctx, store, workspaceID, metaID); got != 4 {
		t.Fatalf("expected stale SUBSUMED_BY edges preserved after failed cleanup, got %d", got)
	}
	if got := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.archived"); got != beforeArchived {
		t.Fatalf("expected no archived event after failed orphan cleanup, before=%d after=%d", beforeArchived, got)
	}
	if got := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.dependency.removed"); got != beforeRemoved {
		t.Fatalf("expected no dependency.removed event after failed orphan cleanup, before=%d after=%d", beforeRemoved, got)
	}
	if got := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.condensed"); got != beforeCondensed {
		t.Fatalf("expected no summary false-green after failed orphan cleanup, before=%d after=%d", beforeCondensed, got)
	}
}

func TestRefreshMetaTensionsWithContextRejectsOperationSurfaceMismatchWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-condense-surface-mismatch"
	const operatorID = "operator-meta-condense-mismatch"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	_, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		ActorID:                    operatorID,
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.refresh", "server_rpc", workspaceID, "human", operatorID),
		PromptContextSurface:       "workspace.tension.refresh",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   operatorID,
	})
	if err == nil {
		t.Fatal("expected condense surface mismatch to fail closed")
	}
	assertNoMetaTensionRows(t, ctx, store, workspaceID)
	assertNoMetaTensionRuntimeEvents(t, ctx, store, workspaceID)
	if got := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.condensed"); got != 0 {
		t.Fatalf("expected no condensed summary events after surface mismatch, got %d", got)
	}
	if got := countMetaCondenseRuntimeEvents(t, ctx, store, workspaceID, "tension.dependency.added"); got != 0 {
		t.Fatalf("expected no dependency added events after surface mismatch, got %d", got)
	}
}

func TestRefreshMetaTensionsWithContextRequiresActorWithoutSideEffects(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-meta-condense-missing-actor"
	seedMetaTensionCycleFixture(t, ctx, store, workspaceID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	_, err := store.RefreshMetaTensionsWithContext(ctx, TensionCondenseInput{
		WorkspaceID:                workspaceID,
		PromptContextEnvelope:      BuildWorkspaceTensionPromptContextEnvelope("workspace.tension.condense", "server_rpc", workspaceID, "human", "operator-missing"),
		PromptContextSurface:       "workspace.tension.condense",
		PromptContextPrincipalType: "human",
		PromptContextPrincipalID:   "operator-missing",
	})
	if err == nil {
		t.Fatal("expected missing actor to fail closed")
	}
	assertNoMetaTensionRows(t, ctx, store, workspaceID)
	assertNoMetaTensionRuntimeEvents(t, ctx, store, workspaceID)
}

func seedMetaTensionCycleFixture(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, rec := range []tensionRecordFixture{
		{TensionID: "T1", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 60, SurfaceScore: 30, CreatedAt: now, UpdatedAt: now},
		{TensionID: "T2", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 50, SurfaceScore: 45, CreatedAt: now, UpdatedAt: now},
		{TensionID: "T3", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 25, SurfaceScore: 10, CreatedAt: now, UpdatedAt: now},
	} {
		insertTensionRecordFixture(t, ctx, store, rec)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "T1", "T2", "BLOCKS"); err != nil {
		t.Fatalf("add dependency T1->T2: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "T2", "T3", "BLOCKS"); err != nil {
		t.Fatalf("add dependency T2->T3: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "T3", "T1", "BLOCKS"); err != nil {
		t.Fatalf("add dependency T3->T1: %v", err)
	}
}

func countMetaCondenseRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID, eventType string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list %s runtime events: %v", eventType, err)
	}
	return len(events)
}

func countMetaCondenseDependencyEdges(t *testing.T, ctx context.Context, store *Store, workspaceID, metaTensionID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM workspace_tension_dependencies WHERE workspace_id = ? AND depends_on_tension_id = ? AND dependency_type = 'SUBSUMED_BY'`, workspaceID, metaTensionID).Scan(&count); err != nil {
		t.Fatalf("count meta condense dependency edges: %v", err)
	}
	return count
}

func assertTensionLifecycleState(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, expected string) {
	t.Helper()

	var lifecycle string
	if err := store.DB().QueryRowContext(ctx, `SELECT lifecycle_state FROM workspace_tensions WHERE workspace_id = ? AND tension_id = ?`, workspaceID, tensionID).Scan(&lifecycle); err != nil {
		t.Fatalf("query tension lifecycle state: %v", err)
	}
	if lifecycle != expected {
		t.Fatalf("tension %s lifecycle_state = %q, want %q", tensionID, lifecycle, expected)
	}
}

func assertMetaCondensePromptContextEnvelope(t *testing.T, payloadJSON string, expected map[string]string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime event payload: %v", err)
	}
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

func countMetaTensionAuthorityRejectEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) int {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list authority reject events: %v", err)
	}
	return len(events)
}

func assertNoMetaTensionRuntimeEvents(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.emergent",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list tension.emergent runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no tension.emergent runtime events after authority reject, got %+v", events)
	}
}

func assertNoMetaTensionRows(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	tensions, err := store.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TensionType: "meta-tension",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list meta tensions: %v", err)
	}
	if len(tensions) != 0 {
		t.Fatalf("expected no meta tensions after authority reject, got %+v", tensions)
	}
}
