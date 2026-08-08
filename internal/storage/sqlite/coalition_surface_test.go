package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
)

func TestCoalitionFormationSweepUsesCanonicalCreatePathAndStaysIdempotent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-sweep"
		tensionID   = "tension-coalition-sweep"
		taskID      = "task-coalition-sweep"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-surface-seed")

	if err := store.CoalitionFormationSweep(ctx, workspaceID); err != nil {
		t.Fatalf("coalition formation sweep: %v", err)
	}
	if err := store.CoalitionFormationSweep(ctx, workspaceID); err != nil {
		t.Fatalf("coalition formation sweep rerun: %v", err)
	}

	var liveCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING','ACTIVE')`,
		workspaceID,
		tensionID,
	).Scan(&liveCount); err != nil {
		t.Fatalf("count live coalitions: %v", err)
	}
	if liveCount != 1 {
		t.Fatalf("expected exactly one live coalition after repeated sweeps, got %d", liveCount)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if coalition == nil || coalition.SuccessCriterion != "Coalition Surface Target" {
		t.Fatalf("expected sweep to reuse canonical create path, got %+v", coalition)
	}
	if coalition.CreatedEpoch != 0 {
		t.Fatalf("expected first sweep coalition to anchor at epoch 0, got %+v", coalition)
	}
}

func TestCoalitionFormationSweepSkipsIneligibleTensions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID         = "ws-coalition-sweep-ineligible"
		activeTensionID     = "tension-coalition-sweep-active"
		ineligibleTensionID = "tension-coalition-sweep-ineligible"
		activeTaskID        = "task-coalition-sweep-active"
		ineligibleTaskID    = "task-coalition-sweep-ineligible"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, activeTensionID, activeTaskID)
	seedCoalitionSurfaceTension(t, ctx, store, workspaceID, ineligibleTensionID, ineligibleTaskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-surface-seed")

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_tensions SET lifecycle_state = 'GAP', updated_at = ? WHERE workspace_id = ? AND tension_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		ineligibleTensionID,
	); err != nil {
		t.Fatalf("mark tension ineligible: %v", err)
	}

	if err := store.CoalitionFormationSweep(ctx, workspaceID); err != nil {
		t.Fatalf("coalition formation sweep should skip ineligible rows: %v", err)
	}

	for tensionID, want := range map[string]int{
		activeTensionID:     1,
		ineligibleTensionID: 0,
	} {
		var liveCount int
		if err := store.DB().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING','ACTIVE')`,
			workspaceID,
			tensionID,
		).Scan(&liveCount); err != nil {
			t.Fatalf("count live coalitions for %s: %v", tensionID, err)
		}
		if liveCount != want {
			t.Fatalf("live coalition count for %s = %d, want %d", tensionID, liveCount, want)
		}
	}
}

func TestCoalitionJoinOfferStatusAndSeekUseCanonicalCoalitionData(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-surface"
		tensionID   = "tension-coalition-surface"
		taskID      = "task-coalition-surface"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-surface-seed", "agent-surface-probe")

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-surface-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("coalition join offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if coalitionID == "" || coalitionID == "test-coalition-123" {
		t.Fatalf("expected canonical coalition id, got %+v", offer)
	}

	status, err := store.GetCoalitionByWorkspace(ctx, workspaceID)
	if err != nil {
		t.Fatalf("get coalition by workspace: %v", err)
	}
	coalitions, ok := status["coalitions"].([]WorkspaceCoalition)
	if !ok || len(coalitions) != 1 {
		t.Fatalf("expected one live coalition, got %+v", status)
	}
	if coalitions[0].CoalitionID != coalitionID || coalitions[0].TensionID != tensionID {
		t.Fatalf("expected status to surface canonical coalition, got %+v", coalitions[0])
	}
	if len(coalitions[0].Members) != 1 || coalitions[0].Members[0].AgentID != "agent-surface-seed" {
		t.Fatalf("expected status to surface real coalition members, got %+v", coalitions[0].Members)
	}

	seek, err := store.CoalitionSeekQuery(ctx, CoalitionSeekQueryInput{
		WorkspaceID:    workspaceID,
		TaskID:         taskID,
		AgentID:        "agent-surface-probe",
		RequiredSkills: []string{"review"},
		Reason:         "need help",
		Limit:          5,
	})
	if err != nil {
		t.Fatalf("coalition seek query: %v", err)
	}
	matches, ok := seek["matches"].([]map[string]any)
	if !ok || len(matches) != 1 {
		t.Fatalf("expected one canonical match, got %+v", seek)
	}
	match := matches[0]
	matchTension, ok := match["tension"].(TensionRecord)
	if !ok || matchTension.TensionID != tensionID {
		t.Fatalf("expected seek to surface canonical tension, got %+v", match)
	}
	matchCoalition, ok := match["coalition"].(*WorkspaceCoalition)
	if !ok || matchCoalition == nil || matchCoalition.CoalitionID != coalitionID {
		t.Fatalf("expected seek to surface canonical coalition, got %+v", match)
	}
	matchDecision, ok := match["attach_decision"].(AttachmentDecision)
	if !ok || matchDecision.State != AttachmentDecisionAllowed {
		t.Fatalf("expected seek to surface attach-allowed decision envelope, got %+v", match)
	}
}

func TestCoalitionSeekQueryDoesNotFailWhenTaskHasNoEligibleTension(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-seek-no-eligible"
		tensionID   = "tension-coalition-seek-no-eligible"
		taskID      = "task-coalition-seek-no-eligible"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-probe")

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_tensions SET lifecycle_state = 'GAP', updated_at = ? WHERE workspace_id = ? AND tension_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("mark tension ineligible: %v", err)
	}

	seek, err := store.CoalitionSeekQuery(ctx, CoalitionSeekQueryInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-probe",
		Role:        "reviewer",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("coalition seek query should degrade instead of fail: %v", err)
	}
	resolution, ok := seek["target_resolution"].(map[string]any)
	if !ok || resolution["status"] != "not_found" {
		t.Fatalf("expected not_found target resolution, got %+v", seek["target_resolution"])
	}
	if _, ok := seek["matches"].([]map[string]any); !ok {
		t.Fatalf("expected matches slice in degraded response, got %+v", seek)
	}
}

func TestCoalitionJoinOfferRejectsExplicitMismatchAttachment(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-offer-rejected-attach"
		tensionID   = "tension-coalition-offer-rejected-attach"
		taskID      = "task-coalition-offer-rejected-attach"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-fit", "agent-other")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE workspace_tensions
		SET task_ids_json = ?, agent_ids_json = ?, session_ids_json = ?, updated_at = ?
		WHERE workspace_id = ? AND tension_id = ?`,
		`[]`,
		`["agent-other"]`,
		`["sess-other"]`,
		now,
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("tighten tension anchors: %v", err)
	}

	_, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      tensionID,
		AgentID:     "agent-fit",
		Role:        "PRIMARY",
	})
	if !errors.Is(err, ErrCoalitionAttachmentRejected) {
		t.Fatalf("expected rejected attachment envelope error, got %v", err)
	}
	if !strings.Contains(err.Error(), "low_fit_for_explicit_anchors") {
		t.Fatalf("expected explicit anchor mismatch reason in error, got %v", err)
	}
}

func TestAddCoalitionMemberRejectsExplicitMismatchEvenWithProvidedFitNovelty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-direct-add-rejected-attach"
		tensionID   = "tension-coalition-direct-add-rejected-attach"
		taskID      = "task-coalition-direct-add-rejected-attach"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-fit", "agent-other")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE workspace_tensions
		SET task_ids_json = ?, agent_ids_json = ?, session_ids_json = ?, updated_at = ?
		WHERE workspace_id = ? AND tension_id = ?`,
		`[]`,
		`["agent-other"]`,
		`["sess-other"]`,
		now,
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("tighten tension anchors: %v", err)
	}

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "Reject direct mismatch attachment")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	err = store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-fit", 0.95, 0.90)
	if !errors.Is(err, ErrCoalitionAttachmentRejected) {
		t.Fatalf("expected direct add path to respect attach rejection envelope, got %v", err)
	}
}

func TestCoalitionJoinOfferRejectsAmbiguousTaskAnchor(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-ambiguous"
		taskID      = "task-coalition-ambiguous"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, tensionID := range []string{"tension-coalition-ambiguous-a", "tension-coalition-ambiguous-b"} {
		seedCoalitionSurfaceTension(t, ctx, store, workspaceID, tensionID, taskID)
	}

	_, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-ambiguous",
		Role:        "PRIMARY",
	})
	if !errors.Is(err, ErrCoalitionTargetAmbiguous) {
		t.Fatalf("expected ambiguous task anchor error, got %v", err)
	}
}

func TestResolveTensionDisbandsCoalitionWhenTensionBecomesIneligible(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-lifecycle"
		tensionID   = "tension-coalition-lifecycle"
		taskID      = "task-coalition-lifecycle"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-lifecycle")
	createCoalitionSurfaceTask(t, ctx, store, workspaceID, taskID)
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-lifecycle",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("coalition join offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)

	if _, err := store.ResolveTension(ctx, TensionMutationInput{
		WorkspaceID: workspaceID,
		TensionID:   tensionID,
		ActorID:     "agent-lifecycle",
		Reason:      "resolved by lifecycle test",
	}); err != nil {
		t.Fatalf("resolve tension: %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after resolution: %v", err)
	}
	if coalition != nil {
		t.Fatalf("expected resolve path to leave no live coalition projection, got %+v", coalition)
	}

	var status string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		coalitionID,
	).Scan(&status); err != nil {
		t.Fatalf("load coalition status: %v", err)
	}
	if status != "DISBANDED" {
		t.Fatalf("expected resolve path to disband the coalition, got %s", status)
	}
}

func TestAddCoalitionMemberRejectsIneligibleTension(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-ineligible-member"
		tensionID   = "tension-coalition-ineligible-member"
		taskID      = "task-coalition-ineligible-member"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-late")

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("coalition join offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_tensions SET lifecycle_state = 'ARCHIVED', updated_at = ? WHERE workspace_id = ? AND tension_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("archive tension: %v", err)
	}

	_, err = store.AddCoalitionMemberWithHeuristicFactors(ctx, workspaceID, coalitionID, "agent-late")
	if err == nil || !strings.Contains(err.Error(), "not coalition-eligible") {
		t.Fatalf("expected archive tension to reject further coalition membership, got %v", err)
	}
}

func TestGetTensionCoalitionSelectsCanonicalAuthorityWithoutMutatingDuplicateRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-duplicate-authority"
		tensionID          = "tension-coalition-duplicate-authority"
		taskID             = "task-coalition-duplicate-authority"
		canonicalID        = "coalition-canonical-authority"
		duplicateID        = "coalition-shadow-authority"
		canonicalCreatedAt = "2026-04-08T09:00:00Z"
		duplicateCreatedAt = "2026-04-08T09:05:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-alpha", "agent-beta", "agent-shadow")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-alpha", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-beta", "NEAR_REVIEWER", 0.81, 0.39, 4, canonicalCreatedAt)

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 5, duplicateCreatedAt)

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID {
		t.Fatalf("expected canonical coalition %s, got %+v", canonicalID, coalition)
	}
	if len(coalition.Members) != 2 {
		t.Fatalf("expected canonical coalition members to survive, got %+v", coalition.Members)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "FORMING",
	})
}

func TestCoalitionSeekRepeatedReadsKeepShadowDriftUntouched(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-seek-repeated-drift"
		tensionID          = "tension-coalition-seek-repeated-drift"
		taskID             = "task-coalition-seek-repeated-drift"
		canonicalID        = "coalition-seek-repeated-canonical"
		duplicateID        = "coalition-seek-repeated-shadow"
		canonicalCreatedAt = "2026-04-09T15:00:00Z"
		duplicateCreatedAt = "2026-04-09T15:03:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-probe")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 4, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 5, duplicateCreatedAt)

	canonicalBefore := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, canonicalID)
	shadowBefore := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, duplicateID)

	for readIdx := 0; readIdx < 2; readIdx++ {
		seek, err := store.CoalitionSeekQuery(ctx, CoalitionSeekQueryInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			AgentID:     "agent-probe",
			Role:        "REVIEWER",
			Limit:       5,
		})
		if err != nil {
			t.Fatalf("coalition seek query on read %d: %v", readIdx+1, err)
		}
		matches, ok := seek["matches"].([]map[string]any)
		if !ok || len(matches) != 1 {
			t.Fatalf("expected one canonical seek match on read %d, got %+v", readIdx+1, seek)
		}
		matchCoalition, ok := matches[0]["coalition"].(*WorkspaceCoalition)
		if !ok || matchCoalition == nil || matchCoalition.CoalitionID != canonicalID {
			t.Fatalf("expected canonical coalition %s on read %d, got %+v", canonicalID, readIdx+1, matches[0])
		}
		matchIntegrity, ok := matches[0]["coalition_integrity"].(WorkspaceCoalitionIntegrityReport)
		if !ok {
			t.Fatalf("expected coalition integrity report on read %d, got %+v", readIdx+1, matches[0]["coalition_integrity"])
		}
		if matchIntegrity.State != WorkspaceCoalitionIntegrityDrift {
			t.Fatalf("expected repeated seek to surface drift on read %d, got %+v", readIdx+1, matchIntegrity)
		}
		if len(matchIntegrity.Items) != 1 || len(matchIntegrity.Items[0].ShadowCoalitionIDs) != 1 || matchIntegrity.Items[0].ShadowCoalitionIDs[0] != duplicateID {
			t.Fatalf("expected repeated seek integrity to preserve shadow coalition %s on read %d, got %+v", duplicateID, readIdx+1, matchIntegrity)
		}
	}

	canonicalAfter := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, canonicalID)
	shadowAfter := loadCoalitionStatusSnapshot(t, ctx, store, workspaceID, duplicateID)
	if canonicalAfter != canonicalBefore {
		t.Fatalf("expected repeated seek reads to avoid mutating canonical row, before=%+v after=%+v", canonicalBefore, canonicalAfter)
	}
	if shadowAfter != shadowBefore {
		t.Fatalf("expected repeated seek reads to avoid mutating shadow row, before=%+v after=%+v", shadowBefore, shadowAfter)
	}
}

func TestCoalitionJoinOfferDoesNotAttachToNewerEmptyDuplicate(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-offer-authority"
		tensionID          = "tension-coalition-offer-authority"
		taskID             = "task-coalition-offer-authority"
		canonicalID        = "coalition-offer-canonical"
		duplicateID        = "coalition-offer-shadow"
		canonicalCreatedAt = "2026-04-08T10:00:00Z"
		duplicateCreatedAt = "2026-04-08T10:03:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-probe")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "FORMING", 6, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.77, 0.33, 6, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 7, duplicateCreatedAt)

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-probe",
		Role:        "reviewer",
	})
	if err != nil {
		t.Fatalf("coalition join offer: %v", err)
	}
	if got, _ := offer["coalition_id"].(string); got != canonicalID {
		t.Fatalf("expected offer to reuse canonical coalition %s, got %+v", canonicalID, offer)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after offer: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID {
		t.Fatalf("expected canonical coalition after offer, got %+v", coalition)
	}
	if len(coalition.Members) != 2 {
		t.Fatalf("expected offer to attach into canonical coalition, got %+v", coalition.Members)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "DISBANDED",
	})
}

func TestCoalitionInviteRejectsShadowCoalitionIDAfterReconciliation(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-invite-shadow-id"
		tensionID          = "tension-coalition-invite-shadow-id"
		taskID             = "task-coalition-invite-shadow-id"
		canonicalID        = "coalition-invite-canonical"
		duplicateID        = "coalition-invite-shadow"
		canonicalCreatedAt = "2026-04-08T10:10:00Z"
		duplicateCreatedAt = "2026-04-08T10:12:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-target")
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 6, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.79, 0.31, 6, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 7, duplicateCreatedAt)

	err := store.CoalitionInviteEvent(ctx, CoalitionInviteEventInput{
		WorkspaceID: workspaceID,
		CoalitionID: duplicateID,
		AgentID:     "agent-target",
		InvitedBy:   "agent-seed",
		Role:        "REVIEWER",
	})
	if !errors.Is(err, ErrCoalitionExpired) {
		t.Fatalf("expected stale shadow coalition id to fail closed, got %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after stale invite: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID {
		t.Fatalf("expected canonical coalition to survive stale invite, got %+v", coalition)
	}
	if len(coalition.Members) != 1 || coalition.Members[0].AgentID != "agent-seed" {
		t.Fatalf("expected stale invite to avoid mutating canonical membership, got %+v", coalition.Members)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "DISBANDED",
	})
}

func TestCoalitionKickRejectsShadowCoalitionIDAfterReconciliation(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-kick-shadow-id"
		tensionID          = "tension-coalition-kick-shadow-id"
		taskID             = "task-coalition-kick-shadow-id"
		canonicalID        = "coalition-kick-canonical"
		duplicateID        = "coalition-kick-shadow"
		canonicalCreatedAt = "2026-04-08T10:20:00Z"
		duplicateCreatedAt = "2026-04-08T10:23:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-target")
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.84, 0.36, 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-target", "NEAR_REVIEWER", 0.77, 0.29, 8, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 9, duplicateCreatedAt)

	err := store.CoalitionKickEvent(ctx, CoalitionKickEventInput{
		WorkspaceID: workspaceID,
		CoalitionID: duplicateID,
		AgentID:     "agent-target",
		KickedBy:    "agent-seed",
		Reason:      "stale duplicate should not mutate canonical coalition",
	})
	if !errors.Is(err, ErrCoalitionExpired) {
		t.Fatalf("expected stale shadow coalition id to fail closed, got %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after stale kick: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID {
		t.Fatalf("expected canonical coalition to survive stale kick, got %+v", coalition)
	}
	if len(coalition.Members) != 2 {
		t.Fatalf("expected stale kick to avoid mutating canonical membership, got %+v", coalition)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "DISBANDED",
	})
}

func TestCoalitionLeaveRejectsShadowCoalitionIDAfterReconciliation(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-leave-shadow-id"
		tensionID          = "tension-coalition-leave-shadow-id"
		taskID             = "task-coalition-leave-shadow-id"
		canonicalID        = "coalition-leave-canonical"
		duplicateID        = "coalition-leave-shadow"
		canonicalCreatedAt = "2026-04-09T12:00:00Z"
		duplicateCreatedAt = "2026-04-09T12:04:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-target")
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.84, 0.36, 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-target", "NEAR_REVIEWER", 0.77, 0.29, 8, canonicalCreatedAt)
	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 9, duplicateCreatedAt)

	err := store.CoalitionJoinLeave(ctx, CoalitionJoinLeaveInput{
		WorkspaceID: workspaceID,
		CoalitionID: duplicateID,
		AgentID:     "agent-target",
		Reason:      "stale duplicate should not mutate canonical coalition",
	})
	if !errors.Is(err, ErrCoalitionExpired) {
		t.Fatalf("expected stale shadow coalition id to fail closed on leave, got %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after stale leave: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID {
		t.Fatalf("expected canonical coalition to survive stale leave, got %+v", coalition)
	}
	if len(coalition.Members) != 2 {
		t.Fatalf("expected stale leave to avoid mutating canonical membership, got %+v", coalition)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "DISBANDED",
	})
}

func TestCoalitionLeaveRejectsNonMemberAgent(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-leave-non-member"
		tensionID   = "tension-coalition-leave-non-member"
		taskID      = "task-coalition-leave-non-member"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed", "agent-outsider")

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)

	err = store.CoalitionJoinLeave(ctx, CoalitionJoinLeaveInput{
		WorkspaceID: workspaceID,
		CoalitionID: coalitionID,
		AgentID:     "agent-outsider",
		Reason:      "outsider should not silently leave",
	})
	if !errors.Is(err, ErrCoalitionActorNotMember) {
		t.Fatalf("expected non-member leave to fail closed, got %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after non-member leave: %v", err)
	}
	if coalition == nil || len(coalition.Members) != 1 || coalition.Members[0].AgentID != "agent-seed" {
		t.Fatalf("expected non-member leave to avoid mutating coalition, got %+v", coalition)
	}
}

func TestCoalitionLeaveDisbandsLastMemberAndReadSurfacesStayEmpty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-last-member-disband"
		tensionID   = "tension-coalition-last-member-disband"
		taskID      = "task-coalition-last-member-disband"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-seed")

	offer, err := store.CoalitionJoinOffer(ctx, CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if coalitionID == "" {
		t.Fatalf("expected coalition id in offer result, got %+v", offer)
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	if err := store.CoalitionJoinLeave(ctx, CoalitionJoinLeaveInput{
		WorkspaceID: workspaceID,
		CoalitionID: coalitionID,
		AgentID:     "agent-seed",
		Reason:      "last member leaves",
	}); err != nil {
		t.Fatalf("leave last coalition member: %v", err)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after last-member leave: %v", err)
	}
	if coalition != nil {
		t.Fatalf("expected last-member leave to remove live coalition projection, got %+v", coalition)
	}

	for readIdx := 0; readIdx < 2; readIdx++ {
		status, err := store.GetCoalitionByWorkspace(ctx, workspaceID)
		if err != nil {
			t.Fatalf("get coalition by workspace after last-member leave (read %d): %v", readIdx+1, err)
		}

		coalitions, ok := status["coalitions"].([]WorkspaceCoalition)
		if !ok {
			t.Fatalf("expected coalition list payload on read %d, got %T", readIdx+1, status["coalitions"])
		}
		if len(coalitions) != 0 {
			t.Fatalf("expected no live coalitions after last-member leave on read %d, got %+v", readIdx+1, coalitions)
		}

		integrity, ok := status["integrity"].(WorkspaceCoalitionIntegrityReport)
		if !ok {
			t.Fatalf("expected integrity report on read %d, got %T", readIdx+1, status["integrity"])
		}
		if integrity.State != WorkspaceCoalitionIntegrityCurrent {
			t.Fatalf("expected current integrity after last-member leave on read %d, got %+v", readIdx+1, integrity)
		}
		if len(integrity.Items) != 0 || len(integrity.IssueCodes) != 0 {
			t.Fatalf("expected no residual integrity drift after disband on read %d, got %+v", readIdx+1, integrity)
		}
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		coalitionID: "DISBANDED",
	})
}

func TestLoadTensionCoalitionOccupancyUsesCanonicalLiveCoalitionOnlyWithoutMutatingRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-occupancy-authority"
		tensionID          = "tension-coalition-occupancy-authority"
		taskID             = "task-coalition-occupancy-authority"
		canonicalID        = "coalition-occupancy-canonical"
		duplicateID        = "coalition-occupancy-shadow"
		canonicalCreatedAt = "2026-04-08T11:00:00Z"
		duplicateCreatedAt = "2026-04-08T11:04:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-a", "GENERATOR", 0.90, 0.40, 8, canonicalCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, canonicalID, "agent-b", "NEAR_REVIEWER", 0.86, 0.37, 8, canonicalCreatedAt)

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 9, duplicateCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, duplicateID, "agent-c", "GENERATOR", 0.61, 0.21, 9, duplicateCreatedAt)

	occupancy, err := store.loadTensionCoalitionOccupancy(ctx, workspaceID, []string{tensionID})
	if err != nil {
		t.Fatalf("load tension coalition occupancy: %v", err)
	}
	if got := occupancy[tensionID]; got != 2 {
		t.Fatalf("expected occupancy to reflect canonical coalition only, got %d", got)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		canonicalID: "ACTIVE",
		duplicateID: "FORMING",
	})
}

func TestLoadTensionCoalitionOccupancyIgnoresIneligibleTensionWithoutMutatingRows(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-occupancy-ineligible"
		tensionID   = "tension-coalition-occupancy-ineligible"
		taskID      = "task-coalition-occupancy-ineligible"
		coalitionID = "coalition-occupancy-ineligible"
		createdAt   = "2026-04-08T12:00:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-archived")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, coalitionID, "FORMING", 10, createdAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, coalitionID, "agent-archived", "GENERATOR", 0.72, 0.26, 10, createdAt)

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_tensions SET lifecycle_state = 'ARCHIVED', updated_at = ? WHERE workspace_id = ? AND tension_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		tensionID,
	); err != nil {
		t.Fatalf("archive tension: %v", err)
	}

	occupancy, err := store.loadTensionCoalitionOccupancy(ctx, workspaceID, []string{tensionID})
	if err != nil {
		t.Fatalf("load tension coalition occupancy: %v", err)
	}
	if got := occupancy[tensionID]; got != 0 {
		t.Fatalf("expected archived tension occupancy to be ignored, got %d", got)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		coalitionID: "FORMING",
	})
}

func TestGetTensionCoalitionPrefersFresherAuthorityOnExactTie(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID      = "ws-coalition-authority-tie"
		tensionID        = "tension-coalition-authority-tie"
		taskID           = "task-coalition-authority-tie"
		olderCoalitionID = "coalition-authority-tie-old"
		newerCoalitionID = "coalition-authority-tie-new"
		olderCreatedAt   = "2026-04-08T13:00:00Z"
		newerCreatedAt   = "2026-04-08T13:02:00Z"
	)
	seedCoalitionSurfaceWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionSurfaceAgents(t, ctx, store, workspaceID, "agent-tie")

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, olderCoalitionID, "FORMING", 10, olderCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, olderCoalitionID, "agent-tie", "GENERATOR", 0.70, 0.22, 10, olderCreatedAt)

	insertCoalitionSurfaceCoalition(t, ctx, store, workspaceID, tensionID, newerCoalitionID, "FORMING", 11, newerCreatedAt)
	insertCoalitionSurfaceMember(t, ctx, store, workspaceID, newerCoalitionID, "agent-tie", "GENERATOR", 0.70, 0.22, 11, newerCreatedAt)

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition on exact tie: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != newerCoalitionID {
		t.Fatalf("expected fresher coalition %s to win exact tie, got %+v", newerCoalitionID, coalition)
	}

	assertCoalitionStatuses(t, ctx, store, workspaceID, map[string]string{
		olderCoalitionID: "FORMING",
		newerCoalitionID: "FORMING",
	})
}

func seedCoalitionSurfaceWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	seedCoalitionSurfaceTension(t, ctx, store, workspaceID, tensionID, taskID)
}

func seedCoalitionSurfaceTension(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, taskID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json,
			agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count, created_at, updated_at
		) VALUES (?, ?, ?, 'gap', 'ACTIVE', 'PENDING', 'Coalition Surface Target', 'Coalition surface regression target',
			'task_id', ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', 60, 60, 1, ?, ?)`,
		tensionID,
		workspaceID,
		"task:"+workspaceID+"/"+taskID,
		taskID,
		`["`+taskID+`"]`,
		now,
		now,
	); err != nil {
		t.Fatalf("insert workspace tension: %v", err)
	}
}

func registerCoalitionSurfaceAgents(t *testing.T, ctx context.Context, store *Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
			Role:        "generalist",
			Status:      "active",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func createCoalitionSurfaceTask(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{
		Nodes: []dag.NodeSpec{
			{NodeID: taskID + "-node", Type: "compute"},
		},
	})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate coalition surface task graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create coalition surface task %s: %v", taskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach coalition surface task %s to workspace %s: %v", taskID, workspaceID, err)
	}
}

func insertCoalitionSurfaceCoalition(t *testing.T, ctx context.Context, store *Store, workspaceID, tensionID, coalitionID, status string, createdEpoch int, createdAt string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalitions (
			coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		coalitionID,
		workspaceID,
		tensionID,
		"Coalition Surface Target",
		0.75,
		3,
		status,
		createdEpoch,
		createdAt,
		createdAt,
	); err != nil {
		t.Fatalf("insert coalition %s: %v", coalitionID, err)
	}
}

func insertCoalitionSurfaceMember(t *testing.T, ctx context.Context, store *Store, workspaceID, coalitionID, agentID, role string, fitScore, noveltyScore float64, minStayUntilEpoch int, joinedAt string) {
	t.Helper()
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalition_members (
			coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		coalitionID,
		workspaceID,
		agentID,
		role,
		fitScore,
		noveltyScore,
		minStayUntilEpoch,
		joinedAt,
	); err != nil {
		t.Fatalf("insert coalition member %s for %s: %v", agentID, coalitionID, err)
	}
}

func assertCoalitionStatuses(t *testing.T, ctx context.Context, store *Store, workspaceID string, expected map[string]string) {
	t.Helper()
	for coalitionID, wantStatus := range expected {
		var gotStatus string
		if err := store.DB().QueryRowContext(ctx,
			`SELECT status FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
			workspaceID,
			coalitionID,
		).Scan(&gotStatus); err != nil {
			t.Fatalf("load coalition status %s: %v", coalitionID, err)
		}
		if gotStatus != wantStatus {
			t.Fatalf("expected coalition %s status %s, got %s", coalitionID, wantStatus, gotStatus)
		}
	}
}

type coalitionStatusSnapshot struct {
	Status    string
	UpdatedAt string
}

func loadCoalitionStatusSnapshot(t *testing.T, ctx context.Context, store *Store, workspaceID, coalitionID string) coalitionStatusSnapshot {
	t.Helper()
	var snapshot coalitionStatusSnapshot
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, updated_at FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		coalitionID,
	).Scan(&snapshot.Status, &snapshot.UpdatedAt); err != nil {
		t.Fatalf("load coalition snapshot %s: %v", coalitionID, err)
	}
	return snapshot
}
