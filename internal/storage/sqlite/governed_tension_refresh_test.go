package sqlite

import (
	"context"
	"testing"
)

func TestEnsureGovernedTensionRefreshesEvidenceAndAuditLineageOnDuplicateHit(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-governed-tension-refresh"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	firstInput := EnsureGovernedTensionInput{
		WorkspaceID:    workspaceID,
		TensionType:    "failure",
		ProtoClusterID: "cluster-1",
		AnchorRef:      "entity-1",
		Title:          "Pathological failure",
		Summary:        "First governed tension hit",
		EvidenceRefs:   []string{"evt-1"},
	}
	if err := store.EnsureGovernedTension(ctx, firstInput); err != nil {
		t.Fatalf("ensure governed tension first hit: %v", err)
	}

	before, err := store.GetTension(ctx, workspaceID, buildTensionID(workspaceID, "failure", "cluster-1", "entity_id", "entity-1", ""))
	if err != nil {
		t.Fatalf("get governed tension after first hit: %v", err)
	}
	if before.Tension.EvidenceCount == 0 || len(before.Evidence) == 0 {
		t.Fatalf("expected evidence to be recorded on first governed hit, got %+v", before)
	}

	secondInput := firstInput
	secondInput.Summary = "Second governed tension hit"
	secondInput.EvidenceRefs = []string{"evt-2"}
	if err := store.EnsureGovernedTension(ctx, secondInput); err != nil {
		t.Fatalf("ensure governed tension duplicate hit: %v", err)
	}

	after, err := store.GetTension(ctx, workspaceID, buildTensionID(workspaceID, "failure", "cluster-1", "entity_id", "entity-1", ""))
	if err != nil {
		t.Fatalf("get governed tension after duplicate hit: %v", err)
	}
	if after.Tension.EvidenceCount <= before.Tension.EvidenceCount {
		t.Fatalf("expected duplicate hit to refresh evidence lineage, before=%+v after=%+v", before.Tension, after.Tension)
	}
	if len(after.Evidence) <= len(before.Evidence) {
		t.Fatalf("expected duplicate hit to preserve or extend evidence rows, before=%+v after=%+v", before.Evidence, after.Evidence)
	}
	if after.Tension.LastRefreshedAt == before.Tension.LastRefreshedAt && after.Tension.UpdatedAt == before.Tension.UpdatedAt {
		t.Fatalf("expected duplicate hit to refresh audit timestamps, before=%+v after=%+v", before.Tension, after.Tension)
	}
}
