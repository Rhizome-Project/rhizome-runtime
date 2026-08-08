package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestRefreshMetaTensions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-meta-test"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Create 4 tensions: T1 -> T2 -> T3 -> T1 (cycle of 3) and T4 standalone
	fixts := []tensionRecordFixture{
		{TensionID: "T1", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 60, SurfaceScore: 30, CreatedAt: now, UpdatedAt: now},
		{TensionID: "T2", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 50, SurfaceScore: 45, CreatedAt: now, UpdatedAt: now},
		{TensionID: "T3", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 25, SurfaceScore: 10, CreatedAt: now, UpdatedAt: now},
		{TensionID: "T4", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, BaseScore: 10, SurfaceScore: 8, CreatedAt: now, UpdatedAt: now},
	}

	for _, rec := range fixts {
		insertTensionRecordFixture(t, ctx, store, rec)
	}

	// Add dependencies to create a cycle T1 -> T2 -> T3 -> T1
	if err := store.AddTensionDependency(ctx, workspaceID, "T1", "T2", "BLOCKS"); err != nil {
		t.Fatalf("AddTensionDependency T1->T2: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "T2", "T3", "BLOCKS"); err != nil {
		t.Fatalf("AddTensionDependency T2->T3: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "T3", "T1", "BLOCKS"); err != nil {
		t.Fatalf("AddTensionDependency T3->T1: %v", err)
	}

	// Calculate Meta Tensions
	if err := store.RefreshMetaTensions(ctx, workspaceID); err != nil {
		t.Fatalf("RefreshMetaTensions: %v", err)
	}

	// Verify the result
	filter := TensionFilter{WorkspaceID: workspaceID}
	allTensions, err := store.ListTensions(ctx, filter)
	if err != nil {
		t.Fatalf("ListTensions: %v", err)
	}

	var metaTensions []TensionRecord
	membersState := make(map[string]string)

	for _, rec := range allTensions {
		if rec.TensionType == "meta-tension" {
			metaTensions = append(metaTensions, rec)
		}
		membersState[rec.TensionID] = rec.LifecycleState
	}

	// Expect exactly 1 meta-tension
	if len(metaTensions) != 1 {
		t.Fatalf("expected 1 meta-tension, got %d", len(metaTensions))
	}

	meta := metaTensions[0]
	if meta.LifecycleState != tensionLifecycleEmergent {
		t.Fatalf("expected meta tension state EMERGENT, got %s", meta.LifecycleState)
	}
	if meta.Kind != "meta" {
		t.Fatalf("expected meta tension kind to be surfaced as meta, got %+v", meta)
	}
	if len(meta.Members) != 3 {
		t.Fatalf("expected meta tension members to be surfaced, got %+v", meta)
	}
	if meta.Title != "Meta-Tension: Cycle of 3 blocked tensions" {
		t.Fatalf("unexpected meta tension title: %s", meta.Title)
	}
	if meta.BaseScore != 85 || meta.SurfaceScore != 77 {
		t.Fatalf("expected aggregated meta scores base=85 surface=77, got %+v", meta)
	}

	// Verify member tensions stay ACTIVE while the meta-tension becomes the
	// higher-level coordination object for the SCC.
	if st := membersState["T1"]; st != tensionLifecycleActive {
		t.Errorf("expected T1 state ACTIVE, got %s", st)
	}
	if st := membersState["T2"]; st != tensionLifecycleActive {
		t.Errorf("expected T2 state ACTIVE, got %s", st)
	}
	if st := membersState["T3"]; st != tensionLifecycleActive {
		t.Errorf("expected T3 state ACTIVE, got %s", st)
	}
	if st := membersState["T4"]; st != tensionLifecycleActive {
		t.Errorf("expected T4 state ACTIVE, got %s", st)
	}

	// Verify Evidence records linking meta-tension to T1, T2, T3
	// Cannot call listTensionEvidence directly since it's unexported. Let's use GetTension.
	tensionDetail, err := store.GetTension(ctx, workspaceID, meta.TensionID)
	if err != nil {
		t.Fatalf("GetTension: %v", err)
	}

	if len(tensionDetail.Evidence) != 3 {
		t.Fatalf("expected 3 evidence records for meta-tension, got %d", len(tensionDetail.Evidence))
	}
	if tensionDetail.Tension.Kind != "meta" {
		t.Fatalf("expected meta detail kind, got %+v", tensionDetail.Tension)
	}
	if len(tensionDetail.Tension.Members) != 3 {
		t.Fatalf("expected meta detail members, got %+v", tensionDetail.Tension)
	}
	if tensionDetail.Tension.BaseImportance <= 0 || tensionDetail.Tension.SurfacedPriority <= 0 || tensionDetail.Tension.VisibilityScore <= 0 {
		t.Fatalf("expected derived structural scores on meta tension, got %+v", tensionDetail.Tension)
	}
	if tensionDetail.Tension.BaseImportance != 0.85 || tensionDetail.Tension.VisibilityScore != 0.906 || tensionDetail.Tension.SurfacedPriority != 0.77 {
		t.Fatalf("expected aggregated structural scores on meta tension, got %+v", tensionDetail.Tension)
	}
	if len(tensionDetail.Dependencies) != 0 || len(tensionDetail.Dependents) != 0 {
		t.Fatalf("expected meta-tension detail to expose members via members only, got dependencies=%+v dependents=%+v", tensionDetail.Dependencies, tensionDetail.Dependents)
	}

	memberEvMap := make(map[string]bool)
	for _, ev := range tensionDetail.Evidence {
		if ev.EvidenceKind == "member_tension" {
			memberEvMap[ev.EvidenceRef] = true
		}
	}

	for _, member := range []string{"T1", "T2", "T3"} {
		if !memberEvMap[member] {
			t.Errorf("expected member %s in evidence map", member)
		}
	}

	memberDetail, err := store.GetTension(ctx, workspaceID, "T1")
	if err != nil {
		t.Fatalf("get member tension detail: %v", err)
	}
	if memberDetail.Tension.Kind != "atomic" {
		t.Fatalf("expected atomic member kind, got %+v", memberDetail.Tension)
	}
	if len(memberDetail.Dependencies) != 1 || memberDetail.Dependencies[0].TensionID != "T3" || memberDetail.Dependencies[0].DependsOnTensionID != "T1" {
		t.Fatalf("expected T1 dependencies to expose incoming blocker T3->T1, got %+v", memberDetail.Dependencies)
	}
	if len(memberDetail.Dependents) != 1 || memberDetail.Dependents[0].TensionID != "T1" || memberDetail.Dependents[0].DependsOnTensionID != "T2" {
		t.Fatalf("expected T1 dependents to expose outgoing edge T1->T2, got %+v", memberDetail.Dependents)
	}
	if len(memberDetail.Tension.BlockedByTensionIDs) != 1 || memberDetail.Tension.BlockedByTensionIDs[0] != "T3" {
		t.Fatalf("expected T1 blocked_by ids to surface T3, got %+v", memberDetail.Tension.BlockedByTensionIDs)
	}
	if len(memberDetail.Tension.BlocksTensionIDs) != 1 || memberDetail.Tension.BlocksTensionIDs[0] != "T2" {
		t.Fatalf("expected T1 blocks ids to surface T2, got %+v", memberDetail.Tension.BlocksTensionIDs)
	}

	// ---------------------------------------------------------
	// Test Cascade Fail-safe Recovery (SUPERSEDED)
	// ---------------------------------------------------------
	if _, err := store.SupersedeTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   meta.TensionID,
		ActorID:     "agent-reviewer",
		Reason:      "Cycle broke independently",
	}); err != nil {
		t.Fatalf("SupersedeTension: %v", err)
	}

	// T1, T2, T3 stay ACTIVE; meta-tension failure does not cascade lifecycle
	// changes back into the constituent atomic tensions.
	for _, member := range []string{"T1", "T2", "T3"} {
		detail, _ := store.GetTension(ctx, workspaceID, member)
		if detail.Tension.LifecycleState != tensionLifecycleActive {
			t.Errorf("expected %s to be ACTIVE after recovery, got %s", member, detail.Tension.LifecycleState)
		}
	}

	// T4 is still ACTIVE
	detailT4, _ := store.GetTension(ctx, workspaceID, "T4")
	if detailT4.Tension.LifecycleState != tensionLifecycleActive {
		t.Errorf("expected T4 state ACTIVE, got %s", detailT4.Tension.LifecycleState)
	}

	// ---------------------------------------------------------
	// Re-run Condensation to ensure structural membership is stable while
	// member tensions stay ACTIVE.
	// ---------------------------------------------------------
	if err := store.RefreshMetaTensions(ctx, workspaceID); err != nil {
		t.Fatalf("RefreshMetaTensions 2nd time: %v", err)
	}

	for _, member := range []string{"T1", "T2", "T3"} {
		detail, _ := store.GetTension(ctx, workspaceID, member)
		if detail.Tension.LifecycleState != tensionLifecycleActive {
			t.Errorf("expected %s to stay ACTIVE after 2nd condensation, got %s", member, detail.Tension.LifecycleState)
		}
	}

	// ---------------------------------------------------------
	// Test Cascade Success Resolution (RESOLVED)
	// ---------------------------------------------------------
	if _, err := store.ConfirmTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   meta.TensionID,
		ActorID:     "system",
		Reason:      "activate before resolve",
	}); err != nil {
		t.Fatalf("ConfirmTension: %v", err)
	}

	if _, err := store.ResolveTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   meta.TensionID,
		ActorID:     "agent-solver",
		Reason:      "Root cause cycle solved",
	}); err != nil {
		t.Fatalf("ResolveTension: %v", err)
	}

	for _, member := range []string{"T1", "T2", "T3"} {
		detail, _ := store.GetTension(ctx, workspaceID, member)
		if detail.Tension.LifecycleState != tensionLifecycleActive {
			t.Errorf("expected %s to remain ACTIVE after meta resolution, got %s", member, detail.Tension.LifecycleState)
		}
	}
}
