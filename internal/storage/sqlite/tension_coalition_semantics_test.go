package sqlite

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestCoalitionSemanticsEnforceCapTenureAndTTL(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-semantics"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c", "agent-d")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:semantics:1"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      "tension:coalition:semantics:2",
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "keep the coalition bounded")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}

	for _, agentID := range []string{"agent-a", "agent-b", "agent-c"} {
		if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agentID, 0.8, 0.5); err != nil {
			t.Fatalf("add coalition member %s: %v", agentID, err)
		}
	}

	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-d", 0.7, 0.4); !errors.Is(err, ErrCoalitionCapacityReached) {
		t.Fatalf("expected capacity error, got %v", err)
	}

	scored, err := store.ListAgentAvailableTensionsScored(ctx, workspaceID, "agent-d")
	if err != nil {
		t.Fatalf("list scored tensions: %v", err)
	}
	if len(scored) != 1 || scored[0].TensionID != "tension:coalition:semantics:2" {
		t.Fatalf("expected saturated coalition to be skipped, got %+v", scored)
	}

	if err := store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-a"); !errors.Is(err, ErrCoalitionMinimumTenureNotMet) {
		t.Fatalf("expected minimum-tenure error, got %v", err)
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	if err := store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-a"); err != nil {
		t.Fatalf("remove after tenure satisfied: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition after removal: %v", err)
	}
	if current == nil || len(current.Members) != 2 {
		t.Fatalf("expected 2 remaining members, got %+v", current)
	}

	for i := 0; i < 2; i++ {
		if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
			t.Fatalf("advance epoch %d: %v", i, err)
		}
	}

	expired, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get expired coalition: %v", err)
	}
	if expired != nil {
		t.Fatalf("expected TTL-expired coalition to disappear, got %+v", expired)
	}

	var status string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM workspace_coalitions WHERE coalition_id = ?`, coalition.CoalitionID).Scan(&status); err != nil {
		t.Fatalf("load coalition status: %v", err)
	}
	if status != "ACTIVE" {
		t.Fatalf("expected read-only TTL projection to leave stored status untouched before cleanup, got %s", status)
	}

	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-d", 0.5, 0.5); !errors.Is(err, ErrCoalitionExpired) {
		t.Fatalf("expected expired coalition error, got %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM workspace_coalitions WHERE coalition_id = ?`, coalition.CoalitionID).Scan(&status); err != nil {
		t.Fatalf("reload coalition status after expired mutation: %v", err)
	}
	if status != "DISBANDED" {
		t.Fatalf("expected expired write path to disband coalition, got %s", status)
	}
}

func TestCoalitionRoleNormalizationKeepsSeedGeneratorStableAcrossRepeatedConcurrentJoins(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-generator"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:generator"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "normalize coalition roles")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-a", 0.9, 0.3); err != nil {
		t.Fatalf("seed generator: %v", err)
	}

	attempts := []string{"agent-a", "agent-b", "agent-c", "agent-b", "agent-c", "agent-a"}
	start := make(chan struct{})
	errs := make(chan error, len(attempts))

	var wg sync.WaitGroup
	for _, agentID := range attempts {
		agentID := agentID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := runCoalitionStormWithRetry(ctx, func() error {
				return store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, agentID, 0.7, 0.4)
			}); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent repeated join failed: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	if current == nil {
		t.Fatalf("expected coalition after repeated concurrent joins")
	}
	if len(current.Members) != 3 {
		t.Fatalf("expected three unique coalition members, got %+v", current.Members)
	}

	rolesByAgent := make(map[string]string, len(current.Members))
	generatorCount := 0
	for _, member := range current.Members {
		if _, exists := rolesByAgent[member.AgentID]; exists {
			t.Fatalf("expected repeated joins to remain idempotent, duplicate member found: %+v", current.Members)
		}
		rolesByAgent[member.AgentID] = member.Role
		if member.Role == "GENERATOR" {
			generatorCount++
		}
	}

	if generatorCount != 1 {
		t.Fatalf("expected exactly one generator after repeated concurrent joins, got %+v", current.Members)
	}
	if rolesByAgent["agent-a"] != "GENERATOR" {
		t.Fatalf("expected seeded earliest member to remain generator, got roles %+v", rolesByAgent)
	}
	if rolesByAgent["agent-b"] == "GENERATOR" || rolesByAgent["agent-c"] == "GENERATOR" {
		t.Fatalf("expected repeated concurrent joins to avoid generator drift, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionRoleNormalizationPrefersMostDistantEligibleFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-far-reviewer"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:far-reviewer"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared", "shared")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only", "g-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared", "shared")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-only", "near-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-1", "far-only-1")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-2", "far-only-2")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "prefer most distant eligible far reviewer")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.2); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near", 0.8, 0.5); err != nil {
		t.Fatalf("add nearer reviewer: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-far", 0.8, 0.9); err != nil {
		t.Fatalf("add farther reviewer: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}

	if rolesByAgent["agent-far"] != "FAR_REVIEWER" {
		t.Fatalf("expected most distant eligible reviewer to normalize to FAR_REVIEWER, got roles %+v", rolesByAgent)
	}
	if rolesByAgent["agent-near"] != "NEAR_REVIEWER" {
		t.Fatalf("expected less-distant eligible reviewer to normalize to NEAR_REVIEWER, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionRoleNormalizationDoesNotTreatMissingOverlapEvidenceAsFar(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-no-evidence"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-reviewer")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:no-evidence"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, "agent-generator", "agent-reviewer")
	if err != nil {
		t.Fatalf("calculate pairwise distance stats without overlap evidence: %v", err)
	}
	if hasEvidence {
		t.Fatalf("expected no overlap evidence for empty reviewer pair, got distance=%f evidence=%v", distance, hasEvidence)
	}
	if distance != 0 {
		t.Fatalf("expected no-evidence distance stats to stay neutral, got %f", distance)
	}

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "missing overlap evidence should stay neutral")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.2); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-reviewer", 0.6, 0.4); err != nil {
		t.Fatalf("add reviewer without overlap evidence: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}

	if rolesByAgent["agent-reviewer"] == "FAR_REVIEWER" {
		t.Fatalf("expected coalition role normalization to keep missing-overlap reviewer near, got roles %+v", rolesByAgent)
	}
	if rolesByAgent["agent-reviewer"] != "NEAR_REVIEWER" {
		t.Fatalf("expected reviewer without overlap evidence to stay NEAR_REVIEWER, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionRoleNormalizationPrefersHighestFitGeneratorOverJoinOrder(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-fit-generator"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-low-fit", "agent-high-fit")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:fit-generator"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "prefer stronger generator candidate")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-low-fit", 0.45, 0.20); err != nil {
		t.Fatalf("add low-fit member: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-high-fit", 0.95, 0.30); err != nil {
		t.Fatalf("add high-fit member: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}

	if rolesByAgent["agent-high-fit"] != "GENERATOR" {
		t.Fatalf("expected highest-fit member to normalize to GENERATOR, got roles %+v", rolesByAgent)
	}
	if rolesByAgent["agent-low-fit"] == "GENERATOR" {
		t.Fatalf("expected lower-fit earlier joiner to normalize away from GENERATOR, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionRoleNormalizationDoesNotExtendUnchangedTenureAfterLaterJoin(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-tenure"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far", "agent-later")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:tenure"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "bottleneck",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	for _, sharedSource := range []string{"shared-a", "shared-b", "shared-c", "shared-d"} {
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-"+sharedSource, sharedSource)
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-"+sharedSource, sharedSource)
	}
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-later", "later-only-a", "later-only-a")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "keep unchanged reviewer tenure stable")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.4); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near", 0.7, 0.4); err != nil {
		t.Fatalf("add near reviewer: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-far", 0.6, 0.2); err != nil {
		t.Fatalf("add initial far reviewer: %v", err)
	}

	initial, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition before epoch increment: %v", err)
	}
	initialRoles := make(map[string]string, len(initial.Members))
	for _, member := range initial.Members {
		initialRoles[member.AgentID] = member.Role
	}
	if initialRoles["agent-near"] != "NEAR_REVIEWER" {
		t.Fatalf("expected seeded reviewer to start as NEAR_REVIEWER before later join, got %+v", initialRoles)
	}
	if initialRoles["agent-far"] != "FAR_REVIEWER" {
		t.Fatalf("expected disjoint reviewer to hold FAR_REVIEWER before later join, got %+v", initialRoles)
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-later", 0.4, 0.1); err != nil {
		t.Fatalf("add later reviewer: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition before stable-tenure removal: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	minStayByAgent := make(map[string]int, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
		minStayByAgent[member.AgentID] = member.MinStayUntilEpoch
	}
	if rolesByAgent["agent-near"] != "NEAR_REVIEWER" {
		t.Fatalf("expected agent-near to remain NEAR_REVIEWER after later far join, got roles=%+v min_stay=%+v", rolesByAgent, minStayByAgent)
	}
	if minStayByAgent["agent-near"] != 1 {
		t.Fatalf("expected unchanged-role reviewer to keep original min-stay after later join, got roles=%+v min_stay=%+v", rolesByAgent, minStayByAgent)
	}

	if err := store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near"); err != nil {
		t.Fatalf("expected unchanged-role member to remain removable after later join, got %v roles=%+v min_stay=%+v", err, rolesByAgent, minStayByAgent)
	}

	current, err = store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition after stable-tenure removal: %v", err)
	}
	rolesByAgent = make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}
	if rolesByAgent["agent-generator"] != "GENERATOR" || rolesByAgent["agent-far"] != "FAR_REVIEWER" {
		t.Fatalf("expected remaining members to stay normalized after removal, got %+v", rolesByAgent)
	}
}

func TestCoalitionRoleDemotionResetsMinStayToCurrentRole(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-role-demotion-tenure"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-seed", "agent-upgrade")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:roles:demotion-tenure"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "demotion should reset tenure to the current normalized role")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-seed", 0.4, 0.2); err != nil {
		t.Fatalf("add seed member: %v", err)
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_coalition_members
		 SET min_stay_until_epoch = ?
		 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
		currentEpoch+4,
		workspaceID,
		coalition.CoalitionID,
		"agent-seed",
	); err != nil {
		t.Fatalf("inflate seed min-stay before demotion: %v", err)
	}

	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-upgrade", 0.95, 0.3); err != nil {
		t.Fatalf("add higher-fit member: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition after demotion: %v", err)
	}

	seedFound := false
	for _, member := range current.Members {
		if member.AgentID != "agent-seed" {
			continue
		}
		seedFound = true
		if member.Role != "NEAR_REVIEWER" {
			t.Fatalf("expected demoted member to normalize to NEAR_REVIEWER, got %+v", member)
		}
		expectedMinStayUntil := currentEpoch + coalitionMinStayEpochsForRole("NEAR_REVIEWER")
		if member.MinStayUntilEpoch != expectedMinStayUntil {
			t.Fatalf("expected demotion to reset min-stay to current-role tenure, got %+v expected=%d", member, expectedMinStayUntil)
		}
	}
	if !seedFound {
		t.Fatalf("expected seed member to remain in coalition after demotion")
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}
	if err := store.RemoveCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-seed"); err != nil {
		t.Fatalf("expected demoted member to be removable after current-role tenure, got %v", err)
	}
}

func TestCreateCoalitionConcurrentCallersConvergeToSingleActiveCoalition(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-create-race"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:create-race"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	ids := make(chan string, callers)

	for idx := 0; idx < callers; idx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var coalition *WorkspaceCoalition
			err := runCoalitionStormWithRetry(ctx, func() error {
				var err error
				coalition, err = store.CreateCoalition(ctx, workspaceID, tensionID, "race-free create")
				return err
			})
			if err != nil {
				errs <- err
				return
			}
			ids <- coalition.CoalitionID
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(ids)

	for err := range errs {
		t.Fatalf("concurrent create coalition failed: %v", err)
	}

	uniqueIDs := make(map[string]struct{}, callers)
	for coalitionID := range ids {
		uniqueIDs[coalitionID] = struct{}{}
	}
	if len(uniqueIDs) != 1 {
		t.Fatalf("expected concurrent creators to converge to one coalition id, got %+v", uniqueIDs)
	}

	var activeCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM workspace_coalitions WHERE workspace_id = ? AND tension_id = ? AND status IN ('FORMING', 'ACTIVE')`,
		workspaceID,
		tensionID,
	).Scan(&activeCount); err != nil {
		t.Fatalf("count active coalitions: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("expected exactly one active/forming coalition, got %d", activeCount)
	}
}

func TestCoalitionSynergyScoreRewardsComplementarityAndFarReviewerDiversity(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-signals"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, tensionID := range []string{"tension:coalition:signals:near", "tension:coalition:signals:far"} {
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			WorkspaceID:    workspaceID,
			TensionID:      tensionID,
			TensionType:    "gap",
			LifecycleState: tensionLifecycleActive,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only", "g-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-only", "near-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-1", "far-only-1")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-2", "far-only-2")

	nearCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:coalition:signals:near", "prefer narrow overlap coalition")
	if err != nil {
		t.Fatalf("create near coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, nearCoalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add near coalition generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, nearCoalition.CoalitionID, "agent-near", 0.9, 0.2); err != nil {
		t.Fatalf("add near coalition reviewer: %v", err)
	}

	farCoalition, err := store.CreateCoalition(ctx, workspaceID, "tension:coalition:signals:far", "prefer complementary coalition")
	if err != nil {
		t.Fatalf("create far coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, farCoalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add far coalition generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, farCoalition.CoalitionID, "agent-far", 0.9, 0.9); err != nil {
		t.Fatalf("add far coalition reviewer: %v", err)
	}

	nearCurrent, err := store.GetTensionCoalition(ctx, workspaceID, "tension:coalition:signals:near")
	if err != nil {
		t.Fatalf("get near coalition: %v", err)
	}
	farCurrent, err := store.GetTensionCoalition(ctx, workspaceID, "tension:coalition:signals:far")
	if err != nil {
		t.Fatalf("get far coalition: %v", err)
	}
	if nearCurrent == nil || farCurrent == nil {
		t.Fatalf("expected both coalitions to exist, near=%+v far=%+v", nearCurrent, farCurrent)
	}

	nearRoles := make(map[string]string, len(nearCurrent.Members))
	for _, member := range nearCurrent.Members {
		nearRoles[member.AgentID] = member.Role
	}
	farRoles := make(map[string]string, len(farCurrent.Members))
	for _, member := range farCurrent.Members {
		farRoles[member.AgentID] = member.Role
	}
	hasFarReviewer := false
	for _, role := range farRoles {
		if role == "FAR_REVIEWER" {
			hasFarReviewer = true
			break
		}
	}
	if !hasFarReviewer {
		t.Fatalf("expected complementary coalition to surface FAR_REVIEWER diversity, got roles %+v", farRoles)
	}
	if farCurrent.SynergyScore <= nearCurrent.SynergyScore {
		t.Fatalf("expected complementary coalition to score above the overlap-heavy coalition, near=%f far=%f near_roles=%+v far_roles=%+v", nearCurrent.SynergyScore, farCurrent.SynergyScore, nearRoles, farRoles)
	}
}

func TestCoalitionGoalSignalPenalizesWeakLinkBelowEqualMeanAlternative(t *testing.T) {
	t.Parallel()

	weakLinkMembers := []WorkspaceCoalitionMember{
		{AgentID: "agent-a", FitScore: 0.9},
		{AgentID: "agent-b", FitScore: 0.9},
		{AgentID: "agent-c", FitScore: 0.3},
	}
	balancedMembers := []WorkspaceCoalitionMember{
		{AgentID: "agent-a", FitScore: 0.9},
		{AgentID: "agent-b", FitScore: 0.6},
		{AgentID: "agent-c", FitScore: 0.6},
	}

	weakLinkGoal := coalitionGoalSignal(weakLinkMembers)
	balancedGoal := coalitionGoalSignal(balancedMembers)
	legacyMean := coalitionMeanMemberSignal(weakLinkMembers, func(member WorkspaceCoalitionMember) float64 {
		return member.FitScore
	})

	expectedWeakLink := clampCoalitionSignal(0.75*legacyMean + 0.25*0.3)
	expectedBalanced := clampCoalitionSignal(0.75*legacyMean + 0.25*0.6)
	if math.Abs(weakLinkGoal-expectedWeakLink) > 1e-9 {
		t.Fatalf("expected weak-link goal blend, got actual=%f expected=%f", weakLinkGoal, expectedWeakLink)
	}
	if math.Abs(balancedGoal-expectedBalanced) > 1e-9 {
		t.Fatalf("expected balanced goal blend, got actual=%f expected=%f", balancedGoal, expectedBalanced)
	}
	if weakLinkGoal >= legacyMean {
		t.Fatalf("expected weak-link-aware goal to stay below legacy mean, weak=%f legacy=%f", weakLinkGoal, legacyMean)
	}
	if balancedGoal <= weakLinkGoal {
		t.Fatalf("expected equal-mean coalition with stronger weakest member to score higher, weak=%f balanced=%f", weakLinkGoal, balancedGoal)
	}
}

func TestCoalitionGoalScoreSignalScalesSparseEvidenceCoverageForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	const goal = 0.8

	twoMember := coalitionGoalScoreSignal(goal, 1.0/3.0, 1.0, 1.0, 2)
	fullEvidence := coalitionGoalScoreSignal(goal, 1.0, 1.0, 1.0, 3)
	sparseEvidence := coalitionGoalScoreSignal(goal, 1.0/3.0, 1.0, 1.0, 3)

	expectedTwoMember := clampCoalitionSignal(goal)
	expectedFull := clampCoalitionSignal(goal)
	expectedSparse := clampCoalitionSignal(goal * (0.85 + 0.15*(1.0/3.0)))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition goal to skip evidence retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full-evidence coalition goal to stay unchanged, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse evidence to mildly damp top-level goal carryover, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if !(sparseEvidence < fullEvidence && fullEvidence == twoMember) {
		t.Fatalf("expected sparse evidence to retain less top-level goal than full evidence, sparse=%f full=%f two_member=%f", sparseEvidence, fullEvidence, twoMember)
	}
}

func TestCoalitionGoalScoreSignalScalesWeakTopologyForFourMemberCoalitions(t *testing.T) {
	t.Parallel()

	const goal = 0.8

	fullTopology := coalitionGoalScoreSignal(goal, 1.0, 1.0, 1.0, 4)
	weakTopology := coalitionGoalScoreSignal(goal, 1.0, 1.0, 0.75, 4)
	expectedFull := clampCoalitionSignal(goal)
	expectedWeak := clampCoalitionSignal(goal * (0.90 + 0.10*0.75))
	if math.Abs(fullTopology-expectedFull) > 1e-9 {
		t.Fatalf("expected full-topology coalition goal to stay unchanged, got actual=%f expected=%f", fullTopology, expectedFull)
	}
	if math.Abs(weakTopology-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak topology to mildly damp top-level goal carryover, got actual=%f expected=%f", weakTopology, expectedWeak)
	}
	if weakTopology >= fullTopology {
		t.Fatalf("expected weak topology to retain less top-level goal than full topology, weak=%f full=%f", weakTopology, fullTopology)
	}
}

func TestCoalitionGoalScoreSignalScalesWeakPairwiseDistanceForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	const goal = 0.8

	twoMember := coalitionGoalScoreSignal(goal, 1.0, 0.4, 1.0, 2)
	fullDistance := coalitionGoalScoreSignal(goal, 1.0, 1.0, 1.0, 3)
	weakDistance := coalitionGoalScoreSignal(goal, 1.0, 0.4, 1.0, 3)

	expectedTwoMember := clampCoalitionSignal(goal)
	expectedFull := clampCoalitionSignal(goal)
	expectedWeak := clampCoalitionSignal(goal * (0.90 + 0.10*0.4))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition goal to skip pairwise-distance retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary coalition goal to stay unchanged, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to mildly damp top-level goal carryover, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance < fullDistance && fullDistance == twoMember) {
		t.Fatalf("expected weak pairwise distance to retain less top-level goal than full distance, weak=%f full=%f two_member=%f", weakDistance, fullDistance, twoMember)
	}
}

func TestCoalitionGoalRoleDiversityRetentionScalesWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionGoalRoleDiversityRetention(0.4, 2, false)
	fullDiversity := coalitionGoalRoleDiversityRetention(1.0, 3, false)
	weakDiversity := coalitionGoalRoleDiversityRetention(0.4, 3, false)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*0.4)
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition goal retention to skip role-diversity scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected full generic role diversity to keep top-level goal unchanged, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak generic role diversity to mildly damp top-level goal carryover, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weak generic role diversity to retain less top-level goal than full diversity, weak=%f full=%f two_member=%f", weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionGoalRoleDiversityRetentionScalesWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionGoalRoleDiversityRetention(0.4, 2, true)
	fullDiversity := coalitionGoalRoleDiversityRetention(1.0, 3, true)
	weakDiversity := coalitionGoalRoleDiversityRetention(0.4, 3, true)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.92 + 0.08*0.4)
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member FAR-reviewer coalition goal retention to skip reviewer-diversity scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected full reviewer diversity to keep top-level goal unchanged, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity to mildly damp FAR top-level goal carryover, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weak reviewer diversity to retain less FAR top-level goal than full diversity, weak=%f full=%f two_member=%f", weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionGoalNoveltyRetentionScalesWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	twoMember := coalitionGoalNoveltyRetention(0.4, 2)
	fullNovelty := coalitionGoalNoveltyRetention(1.0, 3)
	weakNovelty := coalitionGoalNoveltyRetention(0.3, 3)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*0.3)
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition goal retention to skip novelty scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullNovelty-expectedFull) > 1e-9 {
		t.Fatalf("expected full base novelty to keep top-level goal unchanged, got actual=%f expected=%f", fullNovelty, expectedFull)
	}
	if math.Abs(weakNovelty-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak base novelty to mildly damp top-level goal carryover, got actual=%f expected=%f", weakNovelty, expectedWeak)
	}
	if !(weakNovelty < fullNovelty && fullNovelty == twoMember) {
		t.Fatalf("expected weak base novelty to retain less top-level goal than full novelty, weak=%f full=%f two_member=%f", weakNovelty, fullNovelty, twoMember)
	}
}

func TestCoalitionGoalLockRetentionScalesStrongestActiveLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionGoalLockRetention(0, 0, 0)
	roleLocked := coalitionGoalLockRetention(0.4, 0.1, 0.2)
	farLocked := coalitionGoalLockRetention(0.1, 0.2, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.10*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.10*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked goal retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to mildly damp goal retention, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to mildly damp goal retention, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger active locks to retain less top-level goal carryover, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, novelty, roleDiversity float64, memberCount int, hasFarReviewer bool, roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	goalScore := coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, memberCount)
	goalScore *= coalitionGoalRoleDiversityRetention(roleDiversity, memberCount, hasFarReviewer)
	goalScore *= coalitionGoalNoveltyRetention(novelty, memberCount)
	goalScore *= coalitionGoalLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	return goalScore
}

func coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, novelty, roleDiversity float64, memberCount int, hasFarReviewer bool, roleLockPressure, generatorLockPressure, farReviewerLockPressure float64) float64 {
	signal := coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor)
	signal *= coalitionPairwiseDistanceRoleDiversityRetention(roleDiversity, memberCount, hasFarReviewer)
	signal *= coalitionPairwiseDistanceNoveltyRetention(novelty, memberCount)
	signal *= coalitionPairwiseDistanceLockRetention(roleLockPressure, generatorLockPressure, farReviewerLockPressure)
	return signal
}

func TestCoalitionBaseNoveltySignalPenalizesWeakLinkBelowEqualMeanAlternative(t *testing.T) {
	t.Parallel()

	weakLinkMembers := []WorkspaceCoalitionMember{
		{AgentID: "agent-a", NoveltyScore: 0.8},
		{AgentID: "agent-b", NoveltyScore: 0.8},
		{AgentID: "agent-c", NoveltyScore: 0.2},
	}
	balancedMembers := []WorkspaceCoalitionMember{
		{AgentID: "agent-a", NoveltyScore: 0.8},
		{AgentID: "agent-b", NoveltyScore: 0.6},
		{AgentID: "agent-c", NoveltyScore: 0.4},
	}

	weakLinkNovelty := coalitionBaseNoveltySignal(weakLinkMembers)
	balancedNovelty := coalitionBaseNoveltySignal(balancedMembers)
	legacyMean := coalitionMeanMemberSignal(weakLinkMembers, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})

	expectedWeakLink := clampCoalitionSignal(0.80*legacyMean + 0.20*0.2)
	expectedBalanced := clampCoalitionSignal(0.80*legacyMean + 0.20*0.4)
	if math.Abs(weakLinkNovelty-expectedWeakLink) > 1e-9 {
		t.Fatalf("expected weak-link novelty blend, got actual=%f expected=%f", weakLinkNovelty, expectedWeakLink)
	}
	if math.Abs(balancedNovelty-expectedBalanced) > 1e-9 {
		t.Fatalf("expected balanced novelty blend, got actual=%f expected=%f", balancedNovelty, expectedBalanced)
	}
	if weakLinkNovelty >= legacyMean {
		t.Fatalf("expected weak-link novelty signal to stay below legacy mean, weak=%f legacy=%f", weakLinkNovelty, legacyMean)
	}
	if balancedNovelty <= weakLinkNovelty {
		t.Fatalf("expected equal-mean coalition with stronger weakest novelty to score higher, weak=%f balanced=%f", weakLinkNovelty, balancedNovelty)
	}
}

func TestCoalitionNoveltySignalScalesWeakGoalRetention(t *testing.T) {
	t.Parallel()

	const novelty = 0.8

	highGoal := coalitionNoveltySignal(novelty, 0.9, 1.0, 1.0, 1.0, 3)
	weakGoal := coalitionNoveltySignal(novelty, 0.4, 1.0, 1.0, 1.0, 3)
	expectedHigh := clampCoalitionSignal(novelty * (0.5 + 0.5*0.9))
	expectedWeak := clampCoalitionSignal(novelty * (0.5 + 0.5*0.4))

	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected novelty to scale by strong goal retention, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected novelty to scale by weak goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= highGoal {
		t.Fatalf("expected weaker goal to retain less novelty, weak=%f high=%f", weakGoal, highGoal)
	}
}

func TestCoalitionNoveltySignalScalesSparseEvidenceCoverage(t *testing.T) {
	t.Parallel()

	const novelty = 0.8
	const goal = 0.9

	fullEvidence := coalitionNoveltySignal(novelty, goal, 1.0, 1.0, 1.0, 3)
	sparseEvidence := coalitionNoveltySignal(novelty, goal, 1.0/3.0, 1.0, 1.0, 3)
	expectedFull := clampCoalitionSignal(novelty * (0.5 + 0.5*goal))
	expectedSparse := clampCoalitionSignal(novelty * (0.5 + 0.5*goal) * (0.6 + 0.4*(1.0/3.0)))

	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full evidence novelty retention, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse evidence novelty retention, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if sparseEvidence >= fullEvidence {
		t.Fatalf("expected sparse evidence to retain less novelty than fully evidenced coalition, sparse=%f full=%f", sparseEvidence, fullEvidence)
	}
}

func TestCoalitionNoveltySignalScalesWeakPairwiseDistance(t *testing.T) {
	t.Parallel()

	const novelty = 0.8
	const goal = 0.9

	twoMember := coalitionNoveltySignal(novelty, goal, 1.0, 0.4, 1.0, 2)
	fullDistance := coalitionNoveltySignal(novelty, goal, 1.0, 1.0, 1.0, 3)
	weakDistance := coalitionNoveltySignal(novelty, goal, 1.0, 0.4, 1.0, 3)
	expectedTwoMember := clampCoalitionSignal(novelty * (0.5 + 0.5*goal))
	expectedFull := clampCoalitionSignal(novelty * (0.5 + 0.5*goal))
	expectedWeak := clampCoalitionSignal(novelty * (0.5 + 0.5*goal) * (0.90 + 0.10*0.4))

	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip pairwise-distance novelty retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary novelty to stay unchanged, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to mildly damp novelty, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance < fullDistance && fullDistance == twoMember) {
		t.Fatalf("expected weak pairwise distance to retain less novelty than full distance, weak=%f full=%f two_member=%f", weakDistance, fullDistance, twoMember)
	}
}

func TestCoalitionPairwiseDistanceSignalScalesWeakGoalRetention(t *testing.T) {
	t.Parallel()

	const pairwiseDistance = 0.9

	highGoal := coalitionPairwiseDistanceSignal(pairwiseDistance, 1.0, 0.9, 1.0)
	weakGoal := coalitionPairwiseDistanceSignal(pairwiseDistance, 1.0, 0.4, 1.0)
	expectedHigh := clampCoalitionSignal(pairwiseDistance * (0.75 + 0.25*0.9))
	expectedWeak := clampCoalitionSignal(pairwiseDistance * (0.75 + 0.25*0.4))

	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected pairwise distance to scale by strong goal retention, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected pairwise distance to scale by weak goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= highGoal {
		t.Fatalf("expected weaker goal to retain less complementarity distance, weak=%f high=%f", weakGoal, highGoal)
	}
}

func TestCoalitionPairwiseDistanceSignalScalesSparseEvidenceCoverage(t *testing.T) {
	t.Parallel()

	const pairwiseDistance = 0.9
	const goal = 0.9

	fullEvidence := coalitionPairwiseDistanceSignal(pairwiseDistance, 1.0, goal, 1.0)
	sparseEvidence := coalitionPairwiseDistanceSignal(pairwiseDistance, 1.0/3.0, goal, 1.0)
	expectedFull := clampCoalitionSignal(pairwiseDistance * (0.75 + 0.25*goal))
	expectedSparse := clampCoalitionSignal(pairwiseDistance * (0.5 + 0.5*(1.0/3.0)) * (0.75 + 0.25*goal))

	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full-evidence pairwise distance retention, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse evidence to scale pairwise distance retention, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if sparseEvidence >= fullEvidence {
		t.Fatalf("expected sparse evidence to retain less pairwise distance than full evidence, sparse=%f full=%f", sparseEvidence, fullEvidence)
	}
}

func TestCoalitionPairwiseDistanceRoleDiversityRetentionScalesWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionPairwiseDistanceRoleDiversityRetention(2.0/3.0, 2, false)
	fullDiversity := coalitionPairwiseDistanceRoleDiversityRetention(1.0, 3, false)
	weakDiversity := coalitionPairwiseDistanceRoleDiversityRetention(2.0/3.0, 3, false)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(2.0/3.0))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition pairwise-distance retention to skip role-diversity scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected full generic role diversity to keep pairwise-distance unchanged, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak generic role diversity to mildly damp pairwise-distance carryover, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weak generic role diversity to retain less pairwise-distance than full diversity, weak=%f full=%f two_member=%f", weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionPairwiseDistanceRoleDiversityRetentionScalesWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionPairwiseDistanceRoleDiversityRetention(2.0/3.0, 2, true)
	fullDiversity := coalitionPairwiseDistanceRoleDiversityRetention(1.0, 3, true)
	weakDiversity := coalitionPairwiseDistanceRoleDiversityRetention(2.0/3.0, 3, true)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.92 + 0.08*(2.0/3.0))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member FAR-reviewer coalition pairwise-distance retention to skip reviewer-diversity scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected full reviewer diversity to keep pairwise-distance unchanged, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity to mildly damp FAR pairwise-distance carryover, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weak reviewer diversity to retain less FAR pairwise-distance than full diversity, weak=%f full=%f two_member=%f", weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionPairwiseDistanceNoveltyRetentionScalesWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	twoMember := coalitionPairwiseDistanceNoveltyRetention(0.3, 2)
	fullNovelty := coalitionPairwiseDistanceNoveltyRetention(1.0, 3)
	weakNovelty := coalitionPairwiseDistanceNoveltyRetention(0.3, 3)

	expectedTwoMember := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*0.3)
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member coalition pairwise-distance retention to skip novelty scaling, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullNovelty-expectedFull) > 1e-9 {
		t.Fatalf("expected full base novelty to keep pairwise-distance unchanged, got actual=%f expected=%f", fullNovelty, expectedFull)
	}
	if math.Abs(weakNovelty-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak base novelty to mildly damp pairwise-distance carryover, got actual=%f expected=%f", weakNovelty, expectedWeak)
	}
	if !(weakNovelty < fullNovelty && fullNovelty == twoMember) {
		t.Fatalf("expected weak base novelty to retain less pairwise-distance than full novelty, weak=%f full=%f two_member=%f", weakNovelty, fullNovelty, twoMember)
	}
}

func TestCoalitionPairwiseDistanceLockRetentionScalesStrongestActiveLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionPairwiseDistanceLockRetention(0, 0, 0)
	roleLocked := coalitionPairwiseDistanceLockRetention(0.4, 0.1, 0.2)
	farLocked := coalitionPairwiseDistanceLockRetention(0.1, 0.2, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.10*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.10*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked pairwise-distance retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to mildly damp pairwise-distance retention, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to mildly damp pairwise-distance retention, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger active locks to retain less pairwise-distance complementarity, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func TestCoalitionNoveltyLockRetentionScalesStrongestActiveLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionNoveltyLockRetention(0, 0, 0)
	roleLocked := coalitionNoveltyLockRetention(0.4, 0.1, 0.2)
	farLocked := coalitionNoveltyLockRetention(0.1, 0.2, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.15*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.15*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked novelty retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to mildly damp novelty retention, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to mildly damp novelty retention, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger active locks to retain less novelty, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func TestCoalitionNoveltyRoleDiversityRetentionScalesWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionNoveltyRoleDiversityRetention(2.0/3.0, 2, false)
	fullDiversity := coalitionNoveltyRoleDiversityRetention(1.0, 3, false)
	weakDiversity := coalitionNoveltyRoleDiversityRetention(2.0/3.0, 3, false)
	collapsedDiversity := coalitionNoveltyRoleDiversityRetention(1.0/3.0, 3, false)

	if math.Abs(twoMember-1.0) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip novelty role-diversity retention, got %f", twoMember)
	}
	if math.Abs(fullDiversity-1.0) > 1e-9 {
		t.Fatalf("expected full generic role diversity to keep full novelty retention, got %f", fullDiversity)
	}
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(2.0/3.0))
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak generic role diversity to mildly damp novelty retention, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	expectedCollapsed := clampCoalitionSignal(0.90 + 0.10*(1.0/3.0))
	if math.Abs(collapsedDiversity-expectedCollapsed) > 1e-9 {
		t.Fatalf("expected collapsed generic role diversity to damp novelty retention further, got actual=%f expected=%f", collapsedDiversity, expectedCollapsed)
	}
	if !(collapsedDiversity < weakDiversity && weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weaker no-FAR role diversity to retain less novelty, collapsed=%f weak=%f full=%f two_member=%f", collapsedDiversity, weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionNoveltyRoleDiversityRetentionScalesWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionNoveltyRoleDiversityRetention(2.0/3.0, 2, true)
	fullDiversity := coalitionNoveltyRoleDiversityRetention(1.0, 3, true)
	weakDiversity := coalitionNoveltyRoleDiversityRetention(2.0/3.0, 3, true)
	collapsedDiversity := coalitionNoveltyRoleDiversityRetention(1.0/3.0, 3, true)

	if math.Abs(twoMember-1.0) > 1e-9 {
		t.Fatalf("expected two-member FAR coalition to skip reviewer-diversity novelty retention, got %f", twoMember)
	}
	if math.Abs(fullDiversity-1.0) > 1e-9 {
		t.Fatalf("expected full reviewer diversity to keep full FAR novelty retention, got %f", fullDiversity)
	}
	expectedWeak := clampCoalitionSignal(0.92 + 0.08*(2.0/3.0))
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity to mildly damp FAR novelty retention, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	expectedCollapsed := clampCoalitionSignal(0.92 + 0.08*(1.0/3.0))
	if math.Abs(collapsedDiversity-expectedCollapsed) > 1e-9 {
		t.Fatalf("expected collapsed reviewer diversity to damp FAR novelty retention further, got actual=%f expected=%f", collapsedDiversity, expectedCollapsed)
	}
	if !(collapsedDiversity < weakDiversity && weakDiversity < fullDiversity && fullDiversity == twoMember) {
		t.Fatalf("expected weaker FAR reviewer diversity to retain less novelty, collapsed=%f weak=%f full=%f two_member=%f", collapsedDiversity, weakDiversity, fullDiversity, twoMember)
	}
}

func TestCoalitionReviewerDiversityLockRetentionScalesStrongestReviewerLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionReviewerDiversityLockRetention(0, 0)
	roleLocked := coalitionReviewerDiversityLockRetention(0.4, 0.1)
	farLocked := coalitionReviewerDiversityLockRetention(0.1, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.10*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.10*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked reviewer-diversity retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to mildly damp reviewer-diversity retention, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to mildly damp reviewer-diversity retention, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger reviewer locks to retain less reviewer-diversity uplift, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func TestCoalitionReviewerDiversityNoveltyRetentionScalesWeakBaseNoveltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionReviewerDiversityNoveltyRetention(1.0/3.0, 2, true)
	noFarReviewer := coalitionReviewerDiversityNoveltyRetention(1.0/3.0, 3, false)
	fullNovelty := coalitionReviewerDiversityNoveltyRetention(1.0, 3, true)
	weakNovelty := coalitionReviewerDiversityNoveltyRetention(1.0/3.0, 3, true)

	expectedInactive := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.92 + 0.08*(1.0/3.0))
	if math.Abs(twoMember-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member reviewer diversity novelty retention to stay inactive, got actual=%f expected=%f", twoMember, expectedInactive)
	}
	if math.Abs(noFarReviewer-expectedInactive) > 1e-9 {
		t.Fatalf("expected no-FAR reviewer diversity novelty retention to stay inactive, got actual=%f expected=%f", noFarReviewer, expectedInactive)
	}
	if math.Abs(fullNovelty-expectedFull) > 1e-9 {
		t.Fatalf("expected fresh reviewer diversity to keep full novelty retention, got actual=%f expected=%f", fullNovelty, expectedFull)
	}
	if math.Abs(weakNovelty-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak base novelty to mildly damp reviewer-diversity uplift with far reviewer, got actual=%f expected=%f", weakNovelty, expectedWeak)
	}
	if weakNovelty >= fullNovelty {
		t.Fatalf("expected weaker base novelty to retain less reviewer-diversity uplift, weak=%f full=%f", weakNovelty, fullNovelty)
	}
}

func TestCoalitionReviewerDiversityGoalRetentionScalesWeakGoalRetention(t *testing.T) {
	t.Parallel()

	highGoal := coalitionReviewerDiversityGoalRetention(0.9)
	weakGoal := coalitionReviewerDiversityGoalRetention(0.4)

	expectedHigh := clampCoalitionSignal(0.80 + 0.20*0.9)
	expectedWeak := clampCoalitionSignal(0.80 + 0.20*0.4)
	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected reviewer-diversity retention to scale by strong goal retention, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected reviewer-diversity retention to scale by weak goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= highGoal {
		t.Fatalf("expected weaker goal to retain less reviewer-diversity uplift, weak=%f high=%f", weakGoal, highGoal)
	}
}

func TestCoalitionRoleDiversitySignalScalesWeakGoalRetentionWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 2.0 / 3.0

	highGoal := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, 0.9, 1.0, 0, 0, 3, false)
	weakGoal := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, 0.4, 1.0, 0, 0, 3, false)

	expectedHigh := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*0.9))
	expectedWeak := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*0.4))
	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by strong goal retention, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by weak goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= highGoal {
		t.Fatalf("expected weaker goal to retain less generic role diversity, weak=%f high=%f", weakGoal, highGoal)
	}
}

func TestCoalitionRoleDiversitySignalScalesSparseEvidenceCoverageWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 2.0 / 3.0
	const goal = 0.9

	twoMember := coalitionReviewerDiversitySignal(roleDiversity, 1.0/3.0, 1.0, goal, 1.0, 0, 0, 2, false)
	fullEvidence := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 3, false)
	sparseEvidence := coalitionReviewerDiversitySignal(roleDiversity, 1.0/3.0, 1.0, goal, 1.0, 0, 0, 3, false)

	expectedTwoMember := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedFull := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedSparse := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal) * (0.85 + 0.15*(1.0/3.0)))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member generic role diversity to skip evidence retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full-evidence generic role diversity to stay unchanged, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse evidence to mildly damp generic role diversity, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if !(sparseEvidence < fullEvidence && fullEvidence == twoMember) {
		t.Fatalf("expected sparse evidence to retain less generic role diversity than full evidence, sparse=%f full=%f two_member=%f", sparseEvidence, fullEvidence, twoMember)
	}
}

func TestCoalitionRoleDiversitySignalScalesWeakPairwiseDistanceWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 2.0 / 3.0
	const goal = 0.9

	twoMember := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 0.4, goal, 1.0, 0, 0, 2, false)
	fullDistance := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 3, false)
	weakDistance := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 0.4, goal, 1.0, 0, 0, 3, false)

	expectedTwoMember := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedFull := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedWeak := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal) * (0.90 + 0.10*0.4))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member generic role diversity to skip pairwise-distance retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary generic role diversity to stay unchanged, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to mildly damp generic role diversity, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance < fullDistance && fullDistance == twoMember) {
		t.Fatalf("expected weak pairwise distance to retain less generic role diversity than full distance, weak=%f full=%f two_member=%f", weakDistance, fullDistance, twoMember)
	}
}

func TestCoalitionRoleDiversitySignalScalesActiveRoleLockPressureWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 2.0 / 3.0
	const goal = 0.9

	unlocked := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 3, false)
	locked := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0.6, 0, 3, false)

	expectedUnlocked := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedLocked := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal) * (1.0 - 0.10*0.6))
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked generic role-diversity signal to stay unchanged, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(locked-expectedLocked) > 1e-9 {
		t.Fatalf("expected active same-role lock pressure to mildly damp generic role-diversity signal, got actual=%f expected=%f", locked, expectedLocked)
	}
	if locked >= unlocked {
		t.Fatalf("expected active same-role lock pressure to retain less generic role diversity than unlocked path, locked=%f unlocked=%f", locked, unlocked)
	}
}

func TestCoalitionRoleDiversitySignalScalesWeakTopologyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 2.0 / 3.0
	const goal = 0.9

	threeMember := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 2.0/3.0, 0, 0, 3, false)
	fullTopology := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 4, false)
	weakTopology := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 2.0/3.0, 0, 0, 4, false)

	expectedThreeMember := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedFull := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal))
	expectedWeak := clampCoalitionSignal(roleDiversity * (0.85 + 0.15*goal) * (0.90 + 0.10*(2.0/3.0)))
	if math.Abs(threeMember-expectedThreeMember) > 1e-9 {
		t.Fatalf("expected three-member generic role diversity to skip topology retention, got actual=%f expected=%f", threeMember, expectedThreeMember)
	}
	if math.Abs(fullTopology-expectedFull) > 1e-9 {
		t.Fatalf("expected redundancy-backed generic role diversity to stay unchanged, got actual=%f expected=%f", fullTopology, expectedFull)
	}
	if math.Abs(weakTopology-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak topology to mildly damp generic role diversity without far reviewer, got actual=%f expected=%f", weakTopology, expectedWeak)
	}
	if !(weakTopology < fullTopology && fullTopology == threeMember) {
		t.Fatalf("expected weak topology to retain less generic role diversity than full topology, weak=%f full=%f three_member=%f", weakTopology, fullTopology, threeMember)
	}
}

func TestCoalitionRoleDiversityNoveltyRetentionScalesWeakBaseNoveltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	twoMember := coalitionGenericRoleDiversityNoveltyRetention(1.0/3.0, 2)
	fullNovelty := coalitionGenericRoleDiversityNoveltyRetention(1.0, 3)
	weakNovelty := coalitionGenericRoleDiversityNoveltyRetention(1.0/3.0, 3)

	expectedInactive := 1.0
	expectedFull := 1.0
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(1.0/3.0))
	if math.Abs(twoMember-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member generic role-diversity novelty retention to stay inactive, got actual=%f expected=%f", twoMember, expectedInactive)
	}
	if math.Abs(fullNovelty-expectedFull) > 1e-9 {
		t.Fatalf("expected fresh generic role diversity to keep full novelty retention, got actual=%f expected=%f", fullNovelty, expectedFull)
	}
	if math.Abs(weakNovelty-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak base novelty to mildly damp generic role-diversity uplift without far reviewer, got actual=%f expected=%f", weakNovelty, expectedWeak)
	}
	if weakNovelty >= fullNovelty {
		t.Fatalf("expected weaker base novelty to retain less generic role-diversity uplift, weak=%f full=%f", weakNovelty, fullNovelty)
	}
}

func TestCoalitionFarReviewerBonusSignalScalesWeakGoalRetention(t *testing.T) {
	t.Parallel()

	fullGoal := coalitionFarReviewerBonusSignal(1.0, 1.0, 0.9, 1.0, 3)
	weakGoal := coalitionFarReviewerBonusSignal(1.0, 1.0, 0.4, 1.0, 3)

	expectedFull := clampCoalitionSignal(0.15 * (0.75 + 0.25*0.9))
	expectedWeak := clampCoalitionSignal(0.15 * (0.75 + 0.25*0.4))
	if math.Abs(fullGoal-expectedFull) > 1e-9 {
		t.Fatalf("expected far-reviewer bonus to scale by strong goal retention, got actual=%f expected=%f", fullGoal, expectedFull)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected far-reviewer bonus to scale by weak goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= fullGoal {
		t.Fatalf("expected weaker goal to retain less far-reviewer bonus, weak=%f full=%f", weakGoal, fullGoal)
	}
}

func TestCoalitionReviewerDiversitySignalScalesWeakPairwiseDistanceWithFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 1.0
	const goal = 0.9

	twoMember := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 0.4, goal, 1.0, 0, 0, 2, true)
	fullDistance := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 3, true)
	weakDistance := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 0.4, goal, 1.0, 0, 0, 3, true)

	expectedTwoMember := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal))
	expectedFull := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal))
	expectedWeak := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal) * (0.85 + 0.15*0.4))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member reviewer diversity to skip pairwise-distance retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary reviewer diversity to stay unchanged, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to mildly damp reviewer diversity uplift, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance < fullDistance && fullDistance == twoMember) {
		t.Fatalf("expected weak pairwise distance to retain less reviewer diversity than full distance, weak=%f full=%f two_member=%f", weakDistance, fullDistance, twoMember)
	}
}

func TestCoalitionReviewerDiversitySignalScalesSparseEvidenceCoverageWithFarReviewer(t *testing.T) {
	t.Parallel()

	const roleDiversity = 1.0
	const goal = 0.9

	twoMember := coalitionReviewerDiversitySignal(roleDiversity, 1.0/3.0, 1.0, goal, 1.0, 0, 0, 2, true)
	fullEvidence := coalitionReviewerDiversitySignal(roleDiversity, 1.0, 1.0, goal, 1.0, 0, 0, 3, true)
	sparseEvidence := coalitionReviewerDiversitySignal(roleDiversity, 1.0/3.0, 1.0, goal, 1.0, 0, 0, 3, true)

	expectedTwoMember := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal))
	expectedFull := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal))
	expectedSparse := clampCoalitionSignal(roleDiversity * coalitionReviewerDiversityGoalRetention(goal) * (0.90 + 0.10*(1.0/3.0)))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member reviewer diversity to skip evidence retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full-evidence reviewer diversity to stay unchanged, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse evidence to mildly damp reviewer diversity with far reviewer, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if !(sparseEvidence < fullEvidence && fullEvidence == twoMember) {
		t.Fatalf("expected sparse evidence to retain less reviewer diversity than full evidence with far reviewer, sparse=%f full=%f two_member=%f", sparseEvidence, fullEvidence, twoMember)
	}
}

func TestCoalitionFarReviewerBonusSignalScalesWeakPairwiseDistance(t *testing.T) {
	t.Parallel()

	twoMember := coalitionFarReviewerBonusSignal(1.0, 0.4, 0.9, 1.0, 2)
	fullDistance := coalitionFarReviewerBonusSignal(1.0, 1.0, 0.9, 1.0, 3)
	weakDistance := coalitionFarReviewerBonusSignal(1.0, 0.4, 0.9, 1.0, 3)

	expectedTwoMember := clampCoalitionSignal(0.15 * (0.75 + 0.25*0.9))
	expectedFull := clampCoalitionSignal(0.15 * (0.75 + 0.25*0.9))
	expectedWeak := clampCoalitionSignal(0.15 * (0.85 + 0.15*0.4) * (0.75 + 0.25*0.9))
	if math.Abs(twoMember-expectedTwoMember) > 1e-9 {
		t.Fatalf("expected two-member far-reviewer bonus to skip pairwise-distance retention, got actual=%f expected=%f", twoMember, expectedTwoMember)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary far-reviewer bonus to stay unchanged, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to mildly damp far-reviewer bonus, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance < fullDistance && fullDistance == twoMember) {
		t.Fatalf("expected weak pairwise distance to retain less far-reviewer bonus than full distance, weak=%f full=%f two_member=%f", weakDistance, fullDistance, twoMember)
	}
}

func TestCoalitionFarReviewerBonusRoleDiversityRetentionScalesWeakReviewerDiversity(t *testing.T) {
	t.Parallel()

	fullDiversity := coalitionFarReviewerBonusRoleDiversityRetention(1.0)
	mixedDiversity := coalitionFarReviewerBonusRoleDiversityRetention(2.0 / 3.0)
	weakDiversity := coalitionFarReviewerBonusRoleDiversityRetention(1.0 / 3.0)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.85 + 0.15*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.85 + 0.15*(1.0/3.0))
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected full reviewer diversity to keep full far-reviewer bonus retention, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(mixedDiversity-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed reviewer diversity to mildly damp far-reviewer bonus retention, got actual=%f expected=%f", mixedDiversity, expectedMixed)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity to damp far-reviewer bonus retention more strongly, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity < mixedDiversity && mixedDiversity < fullDiversity) {
		t.Fatalf("expected weaker reviewer diversity to retain less far-reviewer bonus, weak=%f mixed=%f full=%f", weakDiversity, mixedDiversity, fullDiversity)
	}
}

func TestCoalitionFarReviewerBonusNoveltyRetentionScalesWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	fullNovelty := coalitionFarReviewerBonusNoveltyRetention(1.0)
	mixedNovelty := coalitionFarReviewerBonusNoveltyRetention(2.0 / 3.0)
	weakNovelty := coalitionFarReviewerBonusNoveltyRetention(1.0 / 3.0)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.90 + 0.10*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(1.0/3.0))
	if math.Abs(fullNovelty-expectedFull) > 1e-9 {
		t.Fatalf("expected full novelty freshness to keep full far-reviewer bonus retention, got actual=%f expected=%f", fullNovelty, expectedFull)
	}
	if math.Abs(mixedNovelty-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed novelty freshness to mildly damp far-reviewer bonus retention, got actual=%f expected=%f", mixedNovelty, expectedMixed)
	}
	if math.Abs(weakNovelty-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak novelty freshness to damp far-reviewer bonus retention more strongly, got actual=%f expected=%f", weakNovelty, expectedWeak)
	}
	if !(weakNovelty < mixedNovelty && mixedNovelty < fullNovelty) {
		t.Fatalf("expected weaker base novelty to retain less far-reviewer bonus, weak=%f mixed=%f full=%f", weakNovelty, mixedNovelty, fullNovelty)
	}
}

func TestCoalitionFarReviewerBonusLockRetentionScalesStrongestReviewerLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionFarReviewerBonusLockRetention(0, 0)
	roleLocked := coalitionFarReviewerBonusLockRetention(0.4, 0.1)
	farLocked := coalitionFarReviewerBonusLockRetention(0.1, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.10*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.10*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked far-reviewer bonus retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to mildly damp far-reviewer bonus retention, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to mildly damp far-reviewer bonus retention, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger reviewer locks to retain less far-reviewer bonus, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func TestCoalitionCoordinationGoalPenaltyScalesWeakGoalRetentionForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationGoalPenalty(0.4, 2)
	highGoal := coalitionCoordinationGoalPenalty(0.9, 3)
	weakGoal := coalitionCoordinationGoalPenalty(0.4, 3)

	expectedInactive := 0.0
	expectedHigh := 0.08 * (1.0 - 0.9)
	expectedWeak := 0.08 * (1.0 - 0.4)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip goal-aware coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected strong goal retention to keep bounded coordination surcharge, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak goal retention to raise bounded coordination surcharge, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if !(weakGoal > highGoal && highGoal > inactive) {
		t.Fatalf("expected weaker goal to pay more coordination surcharge, weak=%f high=%f inactive=%f", weakGoal, highGoal, inactive)
	}
}

func TestCoalitionCoordinationComplementarityPenaltyScalesWeakPairwiseDistanceForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationComplementarityPenalty(0.4, 2)
	fullDistance := coalitionCoordinationComplementarityPenalty(1.0, 3)
	weakDistance := coalitionCoordinationComplementarityPenalty(0.4, 3)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.05 * (1.0 - 0.4)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip complementarity-aware coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary coalition to avoid complementarity-aware coordination surcharge, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to raise bounded coordination surcharge, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance > fullDistance && fullDistance == inactive) {
		t.Fatalf("expected weak pairwise distance to pay more coordination surcharge, weak=%f full=%f inactive=%f", weakDistance, fullDistance, inactive)
	}
}

func TestCoalitionCoordinationRoleDiversityPenaltyScalesWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationRoleDiversityPenalty(2.0/3.0, 2, false)
	fullDiversity := coalitionCoordinationRoleDiversityPenalty(1.0, 3, false)
	weakDiversity := coalitionCoordinationRoleDiversityPenalty(2.0/3.0, 3, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.04 * (1.0 - 2.0/3.0)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip role-diversity-aware coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected fully diverse coalition to avoid role-diversity-aware coordination surcharge, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak role diversity without far reviewer to add bounded coordination surcharge, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity > fullDiversity && fullDiversity == inactive) {
		t.Fatalf("expected weaker role diversity to pay more coordination surcharge, weak=%f full=%f inactive=%f", weakDiversity, fullDiversity, inactive)
	}
}

func TestCoalitionCoordinationReviewerDiversityPenaltyScalesWeakRoleDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationRoleDiversityPenalty(2.0/3.0, 2, true)
	fullDiversity := coalitionCoordinationRoleDiversityPenalty(1.0, 3, true)
	weakDiversity := coalitionCoordinationRoleDiversityPenalty(2.0/3.0, 3, true)
	noFarWeak := coalitionCoordinationRoleDiversityPenalty(2.0/3.0, 3, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.02 * (1.0 - 2.0/3.0)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member FAR-reviewer coalition to skip reviewer-diversity coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullDiversity-expectedFull) > 1e-9 {
		t.Fatalf("expected fully diverse FAR-reviewer coalition to avoid reviewer-diversity coordination surcharge, got actual=%f expected=%f", fullDiversity, expectedFull)
	}
	if math.Abs(weakDiversity-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity with far reviewer to add bounded coordination surcharge, got actual=%f expected=%f", weakDiversity, expectedWeak)
	}
	if !(weakDiversity > fullDiversity && fullDiversity == inactive && weakDiversity < noFarWeak) {
		t.Fatalf("expected weak FAR-reviewer coordination surcharge to stay below no-FAR path, weak=%f full=%f inactive=%f no_far=%f", weakDiversity, fullDiversity, inactive, noFarWeak)
	}
}

func TestCoalitionCoordinationNoveltyPenaltyScalesWeakBaseNoveltyForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationNoveltyPenalty(0.3, 2)
	fresh := coalitionCoordinationNoveltyPenalty(1.0, 3)
	stale := coalitionCoordinationNoveltyPenalty(0.3, 3)

	expectedInactive := 0.0
	expectedFresh := 0.0
	expectedStale := 0.03 * (1.0 - 0.3)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip novelty-aware coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fresh-expectedFresh) > 1e-9 {
		t.Fatalf("expected fresh coalition to avoid novelty-aware coordination surcharge, got actual=%f expected=%f", fresh, expectedFresh)
	}
	if math.Abs(stale-expectedStale) > 1e-9 {
		t.Fatalf("expected weak base novelty to raise bounded coordination surcharge, got actual=%f expected=%f", stale, expectedStale)
	}
	if !(stale > fresh && fresh == inactive) {
		t.Fatalf("expected weaker base novelty to pay more coordination surcharge, stale=%f fresh=%f inactive=%f", stale, fresh, inactive)
	}
}

func TestCoalitionCoordinationLockPenaltyScalesStrongestActiveLockPressureForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	inactive := coalitionCoordinationLockPenalty(0.4, 0.6, 0.8, 2)
	unlocked := coalitionCoordinationLockPenalty(0, 0, 0, 3)
	roleLocked := coalitionCoordinationLockPenalty(0.4, 0.1, 0.2, 3)
	farLocked := coalitionCoordinationLockPenalty(0.1, 0.2, 0.6, 3)

	expectedInactive := 0.0
	expectedUnlocked := 0.0
	expectedRoleLocked := 0.06 * 0.4
	expectedFarLocked := 0.06 * 0.6
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip lock-aware coordination surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked coalition to avoid lock-aware coordination surcharge, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to add bounded coordination surcharge, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to add stronger bounded coordination surcharge, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked > roleLocked && roleLocked > unlocked && unlocked == inactive) {
		t.Fatalf("expected stronger active locks to add more coordination surcharge, far_locked=%f role_locked=%f unlocked=%f inactive=%f", farLocked, roleLocked, unlocked, inactive)
	}
}

func TestCoalitionLockPenaltyEvidenceSurchargeScalesSparseEvidenceWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyEvidenceSurcharge(1.0/3.0, 2, false)
	withFarReviewer := coalitionLockPenaltyEvidenceSurcharge(1.0/3.0, 3, true)
	fullEvidence := coalitionLockPenaltyEvidenceSurcharge(1.0, 3, false)
	sparseEvidence := coalitionLockPenaltyEvidenceSurcharge(1.0/3.0, 3, false)

	expectedInactive := 0.0
	expectedWithFarReviewer := 0.04 * (1.0 - 1.0/3.0)
	expectedFull := 0.0
	expectedSparse := 0.08 * (1.0 - 1.0/3.0)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip evidence-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(withFarReviewer-expectedWithFarReviewer) > 1e-9 {
		t.Fatalf("expected FAR-reviewer coalition to skip generic evidence-aware lock surcharge, got actual=%f expected=%f", withFarReviewer, expectedWithFarReviewer)
	}
	if math.Abs(fullEvidence-expectedFull) > 1e-9 {
		t.Fatalf("expected full-evidence coalition to avoid evidence-aware lock surcharge, got actual=%f expected=%f", fullEvidence, expectedFull)
	}
	if math.Abs(sparseEvidence-expectedSparse) > 1e-9 {
		t.Fatalf("expected sparse no-FAR evidence to add bounded lock surcharge, got actual=%f expected=%f", sparseEvidence, expectedSparse)
	}
	if !(sparseEvidence > withFarReviewer && withFarReviewer > fullEvidence && fullEvidence == inactive) {
		t.Fatalf("expected sparse no-FAR evidence to pay more lock surcharge than FAR-reviewer or full-evidence paths, sparse=%f with_far=%f full=%f inactive=%f", sparseEvidence, withFarReviewer, fullEvidence, inactive)
	}
}

func TestCoalitionLockPenaltyComplementaritySurchargeScalesWeakPairwiseDistanceWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyComplementaritySurcharge(0.4, 2, false)
	fullDistance := coalitionLockPenaltyComplementaritySurcharge(1.0, 3, false)
	weakDistance := coalitionLockPenaltyComplementaritySurcharge(0.4, 3, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.06 * (1.0 - 0.4)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip complementarity-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary coalition to avoid complementarity-aware lock surcharge, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to add bounded no-FAR lock surcharge, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance > fullDistance && fullDistance == inactive) {
		t.Fatalf("expected weak pairwise distance to pay more no-FAR lock surcharge than full/two-member paths, weak=%f full=%f inactive=%f", weakDistance, fullDistance, inactive)
	}
}

func TestCoalitionLockPenaltyComplementaritySurchargeScalesWeakPairwiseDistanceWithFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyComplementaritySurcharge(0.4, 2, true)
	fullDistance := coalitionLockPenaltyComplementaritySurcharge(1.0, 3, true)
	weakDistance := coalitionLockPenaltyComplementaritySurcharge(0.4, 3, true)
	legacyNoFar := coalitionLockPenaltyComplementaritySurcharge(0.4, 3, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.03 * (1.0 - 0.4)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member FAR-reviewer coalition to skip complementarity-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullDistance-expectedFull) > 1e-9 {
		t.Fatalf("expected fully complementary FAR-reviewer coalition to avoid complementarity-aware lock surcharge, got actual=%f expected=%f", fullDistance, expectedFull)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to add bounded FAR-reviewer lock surcharge, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if !(weakDistance > fullDistance && fullDistance == inactive && weakDistance < legacyNoFar) {
		t.Fatalf("expected weak pairwise distance to pay a smaller FAR-reviewer lock surcharge than the no-FAR path, weak=%f full=%f inactive=%f legacy_no_far=%f", weakDistance, fullDistance, inactive, legacyNoFar)
	}
}

func TestCoalitionLockPenaltyTopologySurchargeScalesWeakTopologyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyTopologySurcharge(2.0/3.0, 3, false)
	fullTopology := coalitionLockPenaltyTopologySurcharge(1.0, 4, false)
	weakTopology := coalitionLockPenaltyTopologySurcharge(2.0/3.0, 4, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.05 * (1.0 - 2.0/3.0)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected three-member coalition to skip topology-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullTopology-expectedFull) > 1e-9 {
		t.Fatalf("expected redundancy-backed coalition to avoid topology-aware lock surcharge, got actual=%f expected=%f", fullTopology, expectedFull)
	}
	if math.Abs(weakTopology-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak topology to add bounded no-FAR lock surcharge, got actual=%f expected=%f", weakTopology, expectedWeak)
	}
	if !(weakTopology > fullTopology && fullTopology == inactive) {
		t.Fatalf("expected weak topology to pay more no-FAR lock surcharge than full/inactive paths, weak=%f full=%f inactive=%f", weakTopology, fullTopology, inactive)
	}
}

func TestCoalitionLockPenaltyTopologySurchargeScalesWeakTopologyWithFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyTopologySurcharge(2.0/3.0, 3, true)
	fullTopology := coalitionLockPenaltyTopologySurcharge(1.0, 4, true)
	weakTopology := coalitionLockPenaltyTopologySurcharge(2.0/3.0, 4, true)
	noFarWeak := coalitionLockPenaltyTopologySurcharge(2.0/3.0, 4, false)

	expectedInactive := 0.0
	expectedFull := 0.0
	expectedWeak := 0.03 * (1.0 - 2.0/3.0)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected three-member FAR coalition to skip topology-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fullTopology-expectedFull) > 1e-9 {
		t.Fatalf("expected redundancy-backed FAR coalition to avoid topology-aware lock surcharge, got actual=%f expected=%f", fullTopology, expectedFull)
	}
	if math.Abs(weakTopology-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak topology to add bounded FAR lock surcharge, got actual=%f expected=%f", weakTopology, expectedWeak)
	}
	if !(weakTopology > fullTopology && fullTopology == inactive && weakTopology < noFarWeak) {
		t.Fatalf("expected weak FAR topology surcharge to stay below no-FAR path, weak=%f full=%f inactive=%f no_far=%f", weakTopology, fullTopology, inactive, noFarWeak)
	}
}

func TestCoalitionLockPenaltyNoveltySurchargeScalesWeakBaseNoveltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyNoveltySurcharge(0.3, 2, false)
	fresh := coalitionLockPenaltyNoveltySurcharge(1.0, 3, false)
	stale := coalitionLockPenaltyNoveltySurcharge(0.3, 3, false)

	expectedInactive := 0.0
	expectedFresh := 0.0
	expectedStale := 0.04 * (1.0 - 0.3)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member coalition to skip novelty-aware no-FAR lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fresh-expectedFresh) > 1e-9 {
		t.Fatalf("expected fresh no-FAR coalition to avoid novelty-aware lock surcharge, got actual=%f expected=%f", fresh, expectedFresh)
	}
	if math.Abs(stale-expectedStale) > 1e-9 {
		t.Fatalf("expected weak base novelty to add bounded no-FAR lock surcharge, got actual=%f expected=%f", stale, expectedStale)
	}
	if !(stale > fresh && fresh == inactive) {
		t.Fatalf("expected weaker base novelty to pay more no-FAR lock surcharge, stale=%f fresh=%f inactive=%f", stale, fresh, inactive)
	}
}

func TestCoalitionLockPenaltyNoveltySurchargeScalesWeakBaseNoveltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	inactive := coalitionLockPenaltyNoveltySurcharge(0.3, 2, true)
	fresh := coalitionLockPenaltyNoveltySurcharge(1.0, 3, true)
	stale := coalitionLockPenaltyNoveltySurcharge(0.3, 3, true)
	noFarStale := coalitionLockPenaltyNoveltySurcharge(0.3, 3, false)

	expectedInactive := 0.0
	expectedFresh := 0.0
	expectedStale := 0.02 * (1.0 - 0.3)
	if math.Abs(inactive-expectedInactive) > 1e-9 {
		t.Fatalf("expected two-member FAR-reviewer coalition to skip novelty-aware lock surcharge, got actual=%f expected=%f", inactive, expectedInactive)
	}
	if math.Abs(fresh-expectedFresh) > 1e-9 {
		t.Fatalf("expected fresh FAR-reviewer coalition to avoid novelty-aware lock surcharge, got actual=%f expected=%f", fresh, expectedFresh)
	}
	if math.Abs(stale-expectedStale) > 1e-9 {
		t.Fatalf("expected weak base novelty to add bounded FAR-reviewer lock surcharge, got actual=%f expected=%f", stale, expectedStale)
	}
	if !(stale > fresh && fresh == inactive && stale < noFarStale) {
		t.Fatalf("expected weaker base novelty to pay a smaller FAR-reviewer lock surcharge than no-FAR path, stale=%f fresh=%f inactive=%f no_far=%f", stale, fresh, inactive, noFarStale)
	}
}

func TestCoalitionNoveltySignalDampensWeakTopologyFourMemberCoalitions(t *testing.T) {
	t.Parallel()

	const novelty = 0.8

	connectedThin := coalitionNoveltySignal(novelty, 1.0, 1.0, 1.0, 0.75, 4)
	disconnected := coalitionNoveltySignal(novelty, 1.0, 1.0, 1.0, 2.0/3.0, 4)
	redundant := coalitionNoveltySignal(novelty, 1.0, 1.0, 1.0, 1.0, 4)
	threeMember := coalitionNoveltySignal(novelty, 1.0, 1.0, 1.0, 0.5, 3)

	expectedConnectedThin := clampCoalitionSignal(novelty * (1.0 - 0.15*(1.0-0.75)))
	expectedDisconnected := clampCoalitionSignal(novelty * (1.0 - 0.15*(1.0-2.0/3.0)))
	if math.Abs(connectedThin-expectedConnectedThin) > 1e-9 {
		t.Fatalf("expected connected-thin novelty dampening, got actual=%f expected=%f", connectedThin, expectedConnectedThin)
	}
	if math.Abs(disconnected-expectedDisconnected) > 1e-9 {
		t.Fatalf("expected disconnected novelty dampening, got actual=%f expected=%f", disconnected, expectedDisconnected)
	}
	if redundant != novelty {
		t.Fatalf("expected fully connected redundancy-backed topology to keep full novelty, actual=%f expected=%f", redundant, novelty)
	}
	if threeMember != novelty {
		t.Fatalf("expected 3-member coalition novelty to stay topology-neutral, actual=%f expected=%f", threeMember, novelty)
	}
	if disconnected >= connectedThin {
		t.Fatalf("expected weaker disconnected topology to damp novelty more than connected-thin topology, disconnected=%f connected_thin=%f", disconnected, connectedThin)
	}
}

func TestCoalitionHistoricalPriorSignalScalesWeakGoalRetention(t *testing.T) {
	t.Parallel()

	highGoal := coalitionHistoricalPriorSignal(coalitionHistoricalPriorScale, 1.0, 1.0, 1.0, 1.0, 0.9, 0, 0, 0)
	weakGoal := coalitionHistoricalPriorSignal(coalitionHistoricalPriorScale, 1.0, 1.0, 1.0, 1.0, 0.4, 0, 0, 0)
	expectedHigh := clampCoalitionSignal((0.5 + 0.5*0.9))
	expectedWeak := clampCoalitionSignal((0.5 + 0.5*0.4))

	if math.Abs(highGoal-expectedHigh) > 1e-9 {
		t.Fatalf("expected full prior to scale by strong goal retention, got actual=%f expected=%f", highGoal, expectedHigh)
	}
	if math.Abs(weakGoal-expectedWeak) > 1e-9 {
		t.Fatalf("expected full prior to scale by weak-goal retention, got actual=%f expected=%f", weakGoal, expectedWeak)
	}
	if weakGoal >= highGoal {
		t.Fatalf("expected weaker goal to retain less historical prior, weak=%f high=%f", weakGoal, highGoal)
	}
}

func TestCoalitionHistoricalPriorNoveltyRetentionScalesWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	fresh := coalitionHistoricalPriorNoveltyRetention(1.0)
	stale := coalitionHistoricalPriorNoveltyRetention(0.2)

	expectedFresh := 1.0
	expectedStale := clampCoalitionSignal(0.80 + 0.20*0.2)
	if math.Abs(fresh-expectedFresh) > 1e-9 {
		t.Fatalf("expected fresh novelty retention to stay full, got actual=%f expected=%f", fresh, expectedFresh)
	}
	if math.Abs(stale-expectedStale) > 1e-9 {
		t.Fatalf("expected stale novelty to mildly damp historical prior retention, got actual=%f expected=%f", stale, expectedStale)
	}
	if stale >= fresh {
		t.Fatalf("expected weaker novelty to retain less historical prior, stale=%f fresh=%f", stale, fresh)
	}
}

func TestCoalitionHistoricalPriorRoleDiversityRetentionScalesWeakRoleDiversity(t *testing.T) {
	t.Parallel()

	full := coalitionHistoricalPriorRoleDiversityRetention(1.0, false)
	mixed := coalitionHistoricalPriorRoleDiversityRetention(2.0/3.0, false)
	weak := coalitionHistoricalPriorRoleDiversityRetention(1.0/3.0, false)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.90 + 0.10*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(1.0/3.0))
	if math.Abs(full-expectedFull) > 1e-9 {
		t.Fatalf("expected full role diversity to keep full historical prior retention, got actual=%f expected=%f", full, expectedFull)
	}
	if math.Abs(mixed-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed role diversity to mildly damp historical prior retention, got actual=%f expected=%f", mixed, expectedMixed)
	}
	if math.Abs(weak-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak role diversity to damp historical prior retention more strongly, got actual=%f expected=%f", weak, expectedWeak)
	}
	if !(weak < mixed && mixed < full) {
		t.Fatalf("expected weaker role diversity to retain less historical prior, weak=%f mixed=%f full=%f", weak, mixed, full)
	}
}

func TestCoalitionHistoricalPriorRoleDiversityRetentionScalesWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	full := coalitionHistoricalPriorRoleDiversityRetention(1.0, true)
	mixed := coalitionHistoricalPriorRoleDiversityRetention(2.0/3.0, true)
	weak := coalitionHistoricalPriorRoleDiversityRetention(1.0/3.0, true)
	legacyMixed := coalitionHistoricalPriorRoleDiversityRetention(2.0/3.0, false)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.88 + 0.12*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.88 + 0.12*(1.0/3.0))
	if math.Abs(full-expectedFull) > 1e-9 {
		t.Fatalf("expected full reviewer diversity to keep full FAR historical prior retention, got actual=%f expected=%f", full, expectedFull)
	}
	if math.Abs(mixed-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed reviewer diversity to mildly damp FAR historical prior retention, got actual=%f expected=%f", mixed, expectedMixed)
	}
	if math.Abs(weak-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity to damp FAR historical prior retention more strongly, got actual=%f expected=%f", weak, expectedWeak)
	}
	if !(weak < mixed && mixed < full && mixed < legacyMixed) {
		t.Fatalf("expected weaker FAR reviewer diversity to retain less historical prior and stay below generic retention, weak=%f mixed=%f full=%f legacy_mixed=%f", weak, mixed, full, legacyMixed)
	}
}

func TestCoalitionHistoricalPriorRoleDiversityNoveltyRetentionScalesWeakBaseNoveltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	full := coalitionHistoricalPriorRoleDiversityNoveltyRetention(1.0, false)
	mixed := coalitionHistoricalPriorRoleDiversityNoveltyRetention(2.0/3.0, false)
	weak := coalitionHistoricalPriorRoleDiversityNoveltyRetention(1.0/3.0, false)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.90 + 0.10*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.90 + 0.10*(1.0/3.0))
	if math.Abs(full-expectedFull) > 1e-9 {
		t.Fatalf("expected full novelty freshness to keep full no-FAR historical-prior role-diversity retention, got actual=%f expected=%f", full, expectedFull)
	}
	if math.Abs(mixed-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed novelty freshness to mildly damp no-FAR historical-prior role-diversity retention, got actual=%f expected=%f", mixed, expectedMixed)
	}
	if math.Abs(weak-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak novelty freshness to damp no-FAR historical-prior role-diversity retention more strongly, got actual=%f expected=%f", weak, expectedWeak)
	}
	if !(weak < mixed && mixed < full) {
		t.Fatalf("expected weaker base novelty to retain less no-FAR historical-prior role-diversity carryover, weak=%f mixed=%f full=%f", weak, mixed, full)
	}
}

func TestCoalitionHistoricalPriorRoleDiversityNoveltyRetentionScalesWeakBaseNoveltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	full := coalitionHistoricalPriorRoleDiversityNoveltyRetention(1.0, true)
	mixed := coalitionHistoricalPriorRoleDiversityNoveltyRetention(2.0/3.0, true)
	weak := coalitionHistoricalPriorRoleDiversityNoveltyRetention(1.0/3.0, true)
	legacyMixed := coalitionHistoricalPriorRoleDiversityNoveltyRetention(2.0/3.0, false)

	expectedFull := 1.0
	expectedMixed := clampCoalitionSignal(0.88 + 0.12*(2.0/3.0))
	expectedWeak := clampCoalitionSignal(0.88 + 0.12*(1.0/3.0))
	if math.Abs(full-expectedFull) > 1e-9 {
		t.Fatalf("expected full novelty freshness to keep full FAR historical-prior role-diversity retention, got actual=%f expected=%f", full, expectedFull)
	}
	if math.Abs(mixed-expectedMixed) > 1e-9 {
		t.Fatalf("expected mixed novelty freshness to mildly damp FAR historical-prior role-diversity retention, got actual=%f expected=%f", mixed, expectedMixed)
	}
	if math.Abs(weak-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak novelty freshness to damp FAR historical-prior role-diversity retention more strongly, got actual=%f expected=%f", weak, expectedWeak)
	}
	if !(weak < mixed && mixed < full && mixed < legacyMixed) {
		t.Fatalf("expected weaker FAR novelty freshness to retain less historical prior and stay below generic novelty retention, weak=%f mixed=%f full=%f legacy_mixed=%f", weak, mixed, full, legacyMixed)
	}
}

func TestCoalitionHistoricalPriorComplementarityRetentionScalesWeakPairwiseDistance(t *testing.T) {
	t.Parallel()

	highDistance := coalitionHistoricalPriorComplementarityRetention(0.9)
	weakDistance := coalitionHistoricalPriorComplementarityRetention(0.3)

	expectedHigh := clampCoalitionSignal(0.85 + 0.15*0.9)
	expectedWeak := clampCoalitionSignal(0.85 + 0.15*0.3)
	if math.Abs(highDistance-expectedHigh) > 1e-9 {
		t.Fatalf("expected high pairwise distance to retain more historical prior, got actual=%f expected=%f", highDistance, expectedHigh)
	}
	if math.Abs(weakDistance-expectedWeak) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to retain less historical prior, got actual=%f expected=%f", weakDistance, expectedWeak)
	}
	if weakDistance >= highDistance {
		t.Fatalf("expected weaker pairwise distance to retain less historical prior, weak=%f high=%f", weakDistance, highDistance)
	}
}

func TestCoalitionHistoricalPriorSignalScalesStrongestActiveLockPressure(t *testing.T) {
	t.Parallel()

	unlocked := coalitionHistoricalPriorSignal(coalitionHistoricalPriorScale, 1.0, 1.0, 1.0, 1.0, 1.0, 0, 0, 0)
	roleLocked := coalitionHistoricalPriorSignal(coalitionHistoricalPriorScale, 1.0, 1.0, 1.0, 1.0, 1.0, 0.4, 0.1, 0.2)
	farLocked := coalitionHistoricalPriorSignal(coalitionHistoricalPriorScale, 1.0, 1.0, 1.0, 1.0, 1.0, 0.1, 0.2, 0.6)

	expectedUnlocked := 1.0
	expectedRoleLocked := clampCoalitionSignal(1.0 - 0.25*0.4)
	expectedFarLocked := clampCoalitionSignal(1.0 - 0.25*0.6)
	if math.Abs(unlocked-expectedUnlocked) > 1e-9 {
		t.Fatalf("expected unlocked prior retention to stay full, got actual=%f expected=%f", unlocked, expectedUnlocked)
	}
	if math.Abs(roleLocked-expectedRoleLocked) > 1e-9 {
		t.Fatalf("expected active role lock to scale prior by strongest lock pressure, got actual=%f expected=%f", roleLocked, expectedRoleLocked)
	}
	if math.Abs(farLocked-expectedFarLocked) > 1e-9 {
		t.Fatalf("expected active far-reviewer lock to scale prior by strongest lock pressure, got actual=%f expected=%f", farLocked, expectedFarLocked)
	}
	if !(farLocked < roleLocked && roleLocked < unlocked) {
		t.Fatalf("expected stronger active locks to retain less prior, far_locked=%f role_locked=%f unlocked=%f", farLocked, roleLocked, unlocked)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerBonusByEvidenceCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-bonus-coverage"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for far-reviewer bonus regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	novelty := coalitionBaseNoveltySignal(members)
	legacyNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	expectedComp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)*coalitionPairwiseDistanceLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*legacyNovelty + 0.35*pairwiseDistance + 0.10*roleDiversitySignal + 0.15)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected evidence-scaled far-reviewer bonus, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected evidence-scaled far-reviewer bonus to stay below legacy fixed bonus, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerDiversityByEvidenceCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-diversity-coverage"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for far-reviewer diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	novelty := coalitionBaseNoveltySignal(members)
	legacyNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	expectedComp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)*coalitionPairwiseDistanceLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*legacyNovelty + 0.35*pairwiseDistance + 0.10*roleDiversity + 0.15*evidenceCoverage)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected evidence-scaled far-reviewer diversity uplift, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected evidence-scaled far-reviewer diversity uplift to stay below legacy full-diversity bonus, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerBonusByWeakReviewerDiversity(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-bonus-role-diversity"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator-a", "agent-generator-b", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-a", "generator-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-b", "generator-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-a", "generator-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-b", "generator-b-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator-a", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-generator-b", Role: "GENERATOR", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for far-reviewer role-diversity bonus regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyFarReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members)) * coalitionFarReviewerBonusLockRetention(0, 0)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + legacyFarReviewerBonus)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected far-reviewer bonus to scale by weak reviewer diversity, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak reviewer diversity to retain less far-reviewer bonus than legacy carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesGoalByWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-goal-role-diversity-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-far-a", "agent-far-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-far-a-1", "shared-gfa")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-a", "far-a-generator-1", "shared-gfa")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-far-b-1", "shared-gfb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-b", "far-b-generator-1", "shared-gfb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-a", "far-a-far-b-1", "shared-fab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-b", "far-b-far-a-1", "shared-fab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-far-a", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far-b", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for FAR goal role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), true, 0, 0, 0)
	legacyGoalScore := coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, 1.0, len(members)) * coalitionGoalLockRetention(0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(legacyGoalScore, comp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected top-level goal to scale by weak reviewer diversity with FAR reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak reviewer diversity to retain less top-level goal than legacy FAR carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesPairwiseDistanceByWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-pairwise-distance-role-diversity-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-far-a", "agent-far-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-far-a-1", "shared-gfa")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-a", "far-a-generator-1", "shared-gfa")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-far-b-1", "shared-gfb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-b", "far-b-generator-1", "shared-gfb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-a", "far-a-far-b-1", "shared-fab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far-b", "far-b-far-a-1", "shared-fab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-far-a", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far-b", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for FAR pairwise-distance role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), true, 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), true, 0, 0, 0)
	legacyPairwiseDistanceSignal := coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0) * coalitionPairwiseDistanceLockRetention(0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*legacyPairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected pairwise-distance carryover to scale by weak reviewer diversity with FAR reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak reviewer diversity to retain less pairwise-distance carryover than legacy FAR path, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerBonusByWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-bonus-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	for _, agentID := range []string{"agent-generator", "agent-near", "agent-far"} {
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, agentID, agentID+"-shared-a", "shared-a")
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, agentID, agentID+"-shared-b", "shared-b")
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.2},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for far-reviewer novelty bonus regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyFarReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members)) * coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity) * coalitionFarReviewerBonusLockRetention(0, 0)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + legacyFarReviewerBonus)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected far-reviewer bonus to scale by weak base novelty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less far-reviewer bonus than legacy carryover, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerDiversityByWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-diversity-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	for _, agentID := range []string{"agent-generator", "agent-near", "agent-far"} {
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, agentID, agentID+"-shared-a", "shared-a")
		writeCoalitionRoleMemory(t, ctx, store, workspaceID, agentID, agentID+"-shared-b", "shared-b")
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.2},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for far-reviewer diversity novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyRoleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal + farReviewerBonus)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected far-reviewer diversity to scale by weak base novelty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less far-reviewer diversity than legacy carryover, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesNoveltyByWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-novelty-reviewer-diversity-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator-a", "agent-generator-b", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-a", "generator-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-b", "generator-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-a", "generator-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-b", "generator-b-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator-a", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-generator-b", Role: "GENERATOR", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for FAR novelty reviewer-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyNoveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	legacyComp := clampCoalitionSignal(0.55*legacyNoveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected FAR novelty carryover to scale by weak reviewer diversity, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak reviewer diversity to retain less FAR novelty than legacy carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByEvidenceCoverage(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-prior-coverage"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for historical-prior regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)*coalitionPairwiseDistanceLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(goal, comp, normalizeHistoricalCoalitionPrior(previousScore)*evidenceCoverage, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected evidence-scaled historical prior, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected evidence-scaled historical prior to stay below legacy full prior, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByWeakReviewerDiversityWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-prior-reviewer-diversity-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator-a", "agent-generator-b", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-a", "generator-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-b", "generator-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-a", "generator-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-b", "generator-b-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator-a", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-generator-b", Role: "GENERATOR", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for FAR historical-prior reviewer-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyPrior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	legacyPrior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	legacyPrior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, legacyPrior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected historical prior to scale by weak reviewer diversity with far reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak reviewer diversity to retain less FAR historical prior than generic carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByWeakBaseNoveltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-prior-novelty-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator-a", "agent-generator-b", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-a", "generator-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-b", "generator-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-a", "generator-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-b", "generator-b-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator-a", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-generator-b", Role: "GENERATOR", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for FAR historical-prior novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyPrior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	legacyPrior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, legacyPrior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected historical prior to scale by weak base novelty with far reviewer, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less FAR historical prior than legacy carryover, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreAddsSparseEvidenceCoordinationSurchargeForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-coord-coverage"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for coordination regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)*coalitionPairwiseDistanceLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - 0.10*(1.0-evidenceCoverage)
	legacy := CalculateCoalitionSynergyFromSignals(goal, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected sparse-evidence coordination surcharge for three-member coalition, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected sparse-evidence coordination surcharge to stay below legacy coordination cost, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreAddsComplementarityAwareCoordinationSurchargeForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-coord-complementarity"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for complementarity-aware coordination regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance >= 1.0 {
		t.Fatalf("expected overlap-heavy trio to keep weak pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)*coalitionPairwiseDistanceLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to add bounded coordination surcharge, got actual=%f expected=%f pairwise_distance=%f", actual, expected, pairwiseDistance)
	}
	if actual >= legacy {
		t.Fatalf("expected weak pairwise-distance coordination surcharge to stay below legacy coordination cost, actual=%f legacy=%f pairwise_distance=%f", actual, legacy, pairwiseDistance)
	}
}

func TestCoalitionSynergyScoreAddsRoleDiversityAwareCoordinationSurchargeWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-coordination-role-diversity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-a", "near-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-b", "near-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-only-a", "near-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-only-b", "near-b-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.5},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for role-diversity coordination regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance != 1.0 {
		t.Fatalf("expected fully disjoint trio to keep full pairwise distance, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0) + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak role diversity without far reviewer to add bounded coordination surcharge, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected role-diversity-aware coordination surcharge to stay below legacy coordination cost, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreAddsReviewerDiversityAwareCoordinationSurchargeWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-coordination-role-diversity-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator-a", "agent-generator-b", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-a", "generator-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-a", "generator-a-only-b", "generator-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-a", "generator-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator-b", "generator-b-only-b", "generator-b-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator-a", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-generator-b", Role: "GENERATOR", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for reviewer-diversity coordination regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance != 1.0 {
		t.Fatalf("expected fully disjoint FAR-reviewer trio to keep full pairwise distance, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak reviewer diversity with far reviewer to add bounded coordination surcharge, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected reviewer-diversity-aware coordination surcharge to stay below legacy FAR-reviewer coordination cost, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreAddsNoveltyAwareCoordinationSurchargeForThreeMemberCoalitions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-coordination-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-nab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-nab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for novelty coordination regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, novelty, roleDiversity, len(members), false, 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), false, 0, 0, 0) + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - coalitionCoordinationNoveltyPenalty(novelty, len(members))
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak base novelty to add bounded coordination surcharge, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected novelty-aware coordination surcharge to stay below legacy coordination cost, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesNoveltyByWeakPairwiseDistance(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-novelty-pairwise-distance"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.9},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for novelty complementarity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance >= 1.0 {
		t.Fatalf("expected overlap-heavy trio to keep weak pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, 1.0, 1.0, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false) + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to damp novelty carryover, got actual=%f expected=%f pairwise_distance=%f", actual, expected, pairwiseDistance)
	}
	if actual >= legacy {
		t.Fatalf("expected weak pairwise-distance novelty carryover to stay below legacy novelty term, actual=%f legacy=%f pairwise_distance=%f", actual, legacy, pairwiseDistance)
	}
}

func TestCoalitionSynergyScoreAddsDisconnectedOverlapTopologySurchargeForFourMemberCoalitions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-topology-surcharge"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 2 {
		t.Fatalf("expected full bilateral coverage with disconnected two-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := 1.0 - float64(2-1)/float64(len(members)-1)
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.12 * float64(2-1) / float64(len(members)-1)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - 0.12*float64(2-1)/float64(len(members)-1)
	legacy := CalculateCoalitionSynergyFromSignals(goal, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected disconnected-overlap topology surcharge for four-member coalition, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected disconnected-overlap topology surcharge to stay below legacy coordination cost, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreDampensPairwiseDistanceOnDisconnectedOverlapTopology(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-topology-complementarity"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 2 {
		t.Fatalf("expected full bilateral coverage with disconnected two-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := 1.0 - float64(2-1)/float64(len(members)-1)
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	legacyNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.12 * float64(2-1) / float64(len(members)-1)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*legacyNovelty + 0.35*pairwiseDistance + 0.10*roleDiversitySignal + 0.15*evidenceCoverage)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected disconnected-overlap topology to damp pairwise complementarity, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected disconnected-overlap topology dampening to stay below legacy complementarity, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreDampensPairwiseDistanceOnConnectedThinOverlapTopology(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-thin-connected-topology"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-far-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-a-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 3 {
		t.Fatalf("expected full bilateral coverage with connected three-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := float64(overlapEdges) / float64(len(members))
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	legacyNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.08 * (1.0 - overlapTopologyFactor)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*legacyNovelty + 0.35*pairwiseDistance + 0.10*roleDiversitySignal + 0.15*evidenceCoverage)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected connected thin overlap topology to damp pairwise complementarity, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected connected thin overlap topology dampening to stay below legacy complementarity, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByConnectedThinOverlapTopology(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-thin-connected-prior"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-far-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-a-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 3 {
		t.Fatalf("expected full bilateral coverage with connected three-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := float64(overlapEdges) / float64(len(members))
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, coalitionHasFarReviewer(members))
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, coalitionHasFarReviewer(members))
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.08 * (1.0 - overlapTopologyFactor)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(goal, comp, normalizeHistoricalCoalitionPrior(previousScore)*evidenceCoverage*overlapTopologyFactor, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected connected thin overlap topology to scale historical prior, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected connected thin overlap topology prior scaling to stay below legacy prior carryover, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreAddsConnectedThinOverlapTopologySurchargeForFourMemberCoalitions(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-thin-connected-surcharge"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-far-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-a-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 3 {
		t.Fatalf("expected full bilateral coverage with connected three-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := float64(overlapEdges) / float64(len(members))
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage * overlapTopologyFactor
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.08 * (1.0 - overlapTopologyFactor)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyCoord := coord - 0.08*(1.0-overlapTopologyFactor)
	legacy := CalculateCoalitionSynergyFromSignals(goal, comp, prior, legacyCoord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected connected-thin overlap topology surcharge for four-member coalition, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected connected-thin overlap topology surcharge to stay below legacy coordination cost, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerBonusByConnectedThinOverlapTopology(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-thin-connected-far-bonus"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-far-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-a-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 3 {
		t.Fatalf("expected full bilateral coverage with connected three-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := float64(overlapEdges) / float64(len(members))
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	legacyNovelty := coalitionMeanMemberSignal(members, func(member WorkspaceCoalitionMember) float64 {
		return member.NoveltyScore
	})
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage * overlapTopologyFactor
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.08 * (1.0 - overlapTopologyFactor)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*legacyNovelty + 0.35*(pairwiseDistance*overlapTopologyFactor) + 0.10*roleDiversitySignal + 0.15*evidenceCoverage)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected connected-thin overlap topology to scale far-reviewer bonus, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected connected-thin overlap topology far-reviewer bonus scaling to stay below legacy bonus, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreScalesFarReviewerDiversityByConnectedThinOverlapTopology(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-thin-connected-far-diversity"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-far-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-a-1", "shared-na-f")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8, MinStayUntilEpoch: currentEpoch},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4, MinStayUntilEpoch: currentEpoch},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 3 {
		t.Fatalf("expected full bilateral coverage with connected three-edge overlap topology, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := float64(overlapEdges) / float64(len(members))
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0) * evidenceCoverage * overlapTopologyFactor
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch), len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.08 * (1.0 - overlapTopologyFactor)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	lockPenalty += 0.50 * coalitionActiveRoleLockPressure(members, currentEpoch)
	lockPenalty += 0.45 * coalitionActiveGeneratorLockPressure(members, currentEpoch)
	lockPenalty += 0.40 * coalitionActiveFarReviewerLockPressure(members, currentEpoch)

	comp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))*coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true) + 0.35*coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)) + 0.10*roleDiversitySignal + coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))*coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)*coalitionFarReviewerBonusNoveltyRetention(novelty)*coalitionFarReviewerBonusLockRetention(coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch)))
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), coalitionActiveRoleLockPressure(members, currentEpoch), coalitionActiveGeneratorLockPressure(members, currentEpoch), coalitionActiveFarReviewerLockPressure(members, currentEpoch))
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*coalitionNoveltySignal(novelty, goal, 1.0, 1.0, overlapTopologyFactor, len(members)) + 0.35*(pairwiseDistance*overlapTopologyFactor) + 0.10*(roleDiversity*evidenceCoverage) + 0.15*evidenceCoverage*overlapTopologyFactor)
	legacy := CalculateCoalitionSynergyFromSignals(goal, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected connected-thin overlap topology to scale far-reviewer diversity uplift, got actual=%f expected=%f evidence_coverage=%f topology_factor=%f", actual, expected, evidenceCoverage, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected connected-thin overlap topology far-reviewer diversity scaling to stay below legacy uplift, actual=%f legacy=%f evidence_coverage=%f topology_factor=%f", actual, legacy, evidenceCoverage, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScorePenalizesActiveRoleLockConcentration(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-role-lock"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:signals:role-lock"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only", "g-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only", "near-a-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-only", "near-b-only")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "penalize active same-role lock concentration")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near-a", 0.8, 0.4); err != nil {
		t.Fatalf("add first near reviewer: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near-b", 0.8, 0.4); err != nil {
		t.Fatalf("add second near reviewer: %v", err)
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	for _, forced := range []struct {
		agentID string
		role    string
	}{
		{agentID: "agent-generator", role: "GENERATOR"},
		{agentID: "agent-near-a", role: "NEAR_REVIEWER"},
		{agentID: "agent-near-b", role: "NEAR_REVIEWER"},
	} {
		if _, err := store.DB().ExecContext(ctx,
			`UPDATE workspace_coalition_members
			 SET role = ?
			 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			forced.role,
			workspaceID,
			coalition.CoalitionID,
			forced.agentID,
		); err != nil {
			t.Fatalf("force role %s/%s: %v", forced.agentID, forced.role, err)
		}
	}

	lockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load locked coalition members: %v", err)
	}
	if len(lockedMembers) != 3 {
		t.Fatalf("expected three-member coalition after forced role shape, got %+v", lockedMembers)
	}

	roleCounts := map[string]int{}
	for _, member := range lockedMembers {
		roleCounts[member.Role]++
	}
	if roleCounts["FAR_REVIEWER"] != 0 || roleCounts["NEAR_REVIEWER"] != 2 {
		t.Fatalf("expected same-role reviewer concentration for role-lock regression, got roles %+v", roleCounts)
	}

	lockedPressure := coalitionActiveRoleLockPressure(lockedMembers, currentEpoch)
	if lockedPressure <= 0 {
		t.Fatalf("expected active role-lock pressure for concentrated same-role coalition, members=%+v", lockedMembers)
	}
	lockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, lockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute locked synergy score: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_coalition_members
		 SET min_stay_until_epoch = ?
		 WHERE workspace_id = ? AND coalition_id = ? AND role = 'NEAR_REVIEWER'`,
		currentEpoch,
		workspaceID,
		coalition.CoalitionID,
	); err != nil {
		t.Fatalf("clear near-reviewer min-stay: %v", err)
	}

	unlockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load unlocked coalition members: %v", err)
	}
	unlockedPressure := coalitionActiveRoleLockPressure(unlockedMembers, currentEpoch)
	if unlockedPressure != 0 {
		t.Fatalf("expected cleared min-stay to remove role-lock pressure, members=%+v pressure=%f", unlockedMembers, unlockedPressure)
	}
	unlockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, unlockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute unlocked synergy score: %v", err)
	}
	if unlockedScore <= lockedScore {
		t.Fatalf("expected cleared same-role min-stay to improve synergy score, locked=%f unlocked=%f", lockedScore, unlockedScore)
	}
}

func TestCoalitionSynergyScoreScalesGenericRoleDiversityByEvidenceCoverageWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generic-role-diversity-coverage"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-a", "near-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-b", "near-a-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for generic role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	legacyRoleDiversitySignal := clampCoalitionSignal(roleDiversity * coalitionRoleDiversityGoalRetention(goal, false))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by sparse evidence without far reviewer, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected sparse evidence generic role-diversity scaling to stay below legacy uplift, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreScalesGoalByWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-goal-role-diversity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-nab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-nab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for generic goal role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), false, 0, 0, 0)
	legacyGoalScore := coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, 1.0, len(members)) * coalitionGoalLockRetention(0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(legacyGoalScore, comp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected top-level goal to scale by weak role diversity without FAR reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak role diversity to retain less top-level goal than legacy generic carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesGoalByWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-goal-base-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-1", "shared-gn")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-generator-1", "shared-gn")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-far-1", "shared-gf")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-generator-1", "shared-gf")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-far-1", "shared-nf")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-1", "shared-nf")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for goal base-novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, novelty, roleDiversity, len(members), true, 0, 0, 0)
	legacyGoalScore := coalitionGoalScoreSignal(goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	legacyGoalScore *= coalitionGoalRoleDiversityRetention(roleDiversity, len(members), true)
	legacyGoalScore *= coalitionGoalLockRetention(0, 0, 0)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), true, 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)

	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(legacyGoalScore, comp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected top-level goal to scale by weak base novelty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less top-level goal than legacy carryover, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesPairwiseDistanceByWeakBaseNovelty(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-pairwise-distance-base-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-nab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-nab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for pairwise-distance base-novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, novelty, roleDiversity, len(members), false, 0, 0, 0)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, novelty, roleDiversity, len(members), false, 0, 0, 0)
	legacyPairwiseDistanceSignal := coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0)
	legacyPairwiseDistanceSignal *= coalitionPairwiseDistanceRoleDiversityRetention(roleDiversity, len(members), false)
	legacyPairwiseDistanceSignal *= coalitionPairwiseDistanceLockRetention(0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*legacyPairwiseDistanceSignal + 0.10*roleDiversitySignal)

	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected pairwise-distance signal to scale by weak base novelty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less pairwise-distance carryover than legacy path, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesPairwiseDistanceByWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-pairwise-distance-role-diversity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gna")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gnb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-nab")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-nab")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral evidence coverage for generic pairwise-distance role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), false, 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), false, 0, 0, 0)
	legacyPairwiseDistanceSignal := coalitionPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0) * coalitionPairwiseDistanceLockRetention(0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0) * coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*legacyPairwiseDistanceSignal + 0.10*roleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected pairwise-distance carryover to scale by weak role diversity without FAR reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak role diversity to retain less pairwise-distance carryover than legacy generic path, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesGenericRoleDiversityByWeakPairwiseDistanceWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generic-role-diversity-pairwise-distance"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for generic role-diversity pairwise-distance regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance >= 1.0 {
		t.Fatalf("expected overlap-heavy trio to keep weak pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	legacyRoleDiversitySignal := clampCoalitionSignal(roleDiversity * coalitionRoleDiversityGoalRetention(goal, false))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by weak pairwise distance without far reviewer, got actual=%f expected=%f pairwise_distance=%f", actual, expected, pairwiseDistance)
	}
	if actual >= legacy {
		t.Fatalf("expected weak pairwise-distance generic role-diversity scaling to stay below legacy uplift, actual=%f legacy=%f pairwise_distance=%f", actual, legacy, pairwiseDistance)
	}
}

func TestCoalitionSynergyScoreScalesNoveltyByWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-novelty-role-diversity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-na-nb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-na-nb")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.5},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral coverage for novelty role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)

	legacyNoveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	legacyComp := clampCoalitionSignal(0.55*legacyNoveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected novelty carryover to scale by weak generic role diversity without far reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak generic role diversity novelty carryover to stay below legacy novelty strength, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByWeakRoleDiversityWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-prior-role-diversity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-na-nb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-na-nb")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.5},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral coverage for historical-prior role-diversity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyPrior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, legacyPrior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected historical prior to scale by weak role diversity without far reviewer, got actual=%f expected=%f role_diversity=%f", actual, expected, roleDiversity)
	}
	if actual >= legacy {
		t.Fatalf("expected weak role diversity to retain less historical prior than legacy carryover, actual=%f legacy=%f role_diversity=%f", actual, legacy, roleDiversity)
	}
}

func TestCoalitionSynergyScoreScalesHistoricalPriorByWeakBaseNoveltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-prior-novelty-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-na-nb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-na-nb")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.8},
	}
	previousScore := coalitionHistoricalPriorScale

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, previousScore, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral coverage for no-FAR historical-prior novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyPrior := coalitionHistoricalPriorSignal(previousScore, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	legacyPrior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, legacyPrior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected historical prior to scale by weak base novelty without far reviewer, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty to retain less no-FAR historical prior than legacy carryover, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreScalesGenericRoleDiversityByActiveRoleLockWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generic-role-diversity-role-lock"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:signals:generic-role-diversity-role-lock"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "scale generic role diversity by active role lock without far reviewer")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near-a", 0.8, 0.4); err != nil {
		t.Fatalf("add first near reviewer: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near-b", 0.8, 0.4); err != nil {
		t.Fatalf("add second near reviewer: %v", err)
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	for _, forced := range []struct {
		agentID string
		role    string
	}{
		{agentID: "agent-generator", role: "GENERATOR"},
		{agentID: "agent-near-a", role: "NEAR_REVIEWER"},
		{agentID: "agent-near-b", role: "NEAR_REVIEWER"},
	} {
		if _, err := store.DB().ExecContext(ctx,
			`UPDATE workspace_coalition_members
			 SET role = ?
			 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			forced.role,
			workspaceID,
			coalition.CoalitionID,
			forced.agentID,
		); err != nil {
			t.Fatalf("force role %s/%s: %v", forced.agentID, forced.role, err)
		}
	}

	members, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load coalition members: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected three-member coalition after forced role shape, got %+v", members)
	}

	roleLockPressure := coalitionActiveRoleLockPressure(members, currentEpoch)
	if roleLockPressure <= 0 {
		t.Fatalf("expected active same-role lock pressure on generic role-diversity regression, members=%+v", members)
	}
	generatorLockPressure := coalitionActiveGeneratorLockPressure(members, currentEpoch)

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for generic role-diversity role-lock regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), roleLockPressure, generatorLockPressure, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, roleLockPressure, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	legacyRoleDiversitySignal := clampCoalitionSignal(
		roleDiversity *
			coalitionRoleDiversityGoalRetention(goal, false) *
			coalitionRoleDiversityEvidenceRetention(evidenceCoverage, len(members), false) *
			coalitionRoleDiversityComplementarityRetention(pairwiseDistance, len(members), false),
	)
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, roleLockPressure, generatorLockPressure, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(roleLockPressure, generatorLockPressure, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)
	lockPenalty += 0.50 * roleLockPressure
	lockPenalty += 0.45 * generatorLockPressure

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members)) * coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false) * coalitionNoveltyLockRetention(roleLockPressure, generatorLockPressure, 0)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), false, roleLockPressure, generatorLockPressure, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by active same-role lock pressure without far reviewer, got actual=%f expected=%f role_lock_pressure=%f", actual, expected, roleLockPressure)
	}
	if actual >= legacy {
		t.Fatalf("expected active same-role lock pressure generic role-diversity scaling to stay below legacy uplift, actual=%f legacy=%f role_lock_pressure=%f", actual, legacy, roleLockPressure)
	}
}

func TestCoalitionSynergyScoreScalesGenericRoleDiversityByWeakTopologyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generic-role-diversity-topology"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b", "agent-near-c")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-c-1", "shared-nb-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-c", "near-c-near-b-1", "shared-nb-c")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-c", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.5},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 2 {
		t.Fatalf("expected full bilateral coverage with disconnected two-edge overlap topology for generic role-diversity regression, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := 1.0 - float64(2-1)/float64(len(members)-1)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	legacyRoleDiversitySignal := clampCoalitionSignal(
		roleDiversity *
			coalitionRoleDiversityGoalRetention(goal, false) *
			coalitionRoleDiversityEvidenceRetention(evidenceCoverage, len(members), false) *
			coalitionRoleDiversityComplementarityRetention(pairwiseDistance, len(members), false),
	)
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.10
	coord += 0.12 * float64(2-1) / float64(len(members)-1)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by weak topology without far reviewer, got actual=%f expected=%f topology_factor=%f", actual, expected, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected weak-topology generic role-diversity scaling to stay below legacy uplift, actual=%f legacy=%f topology_factor=%f", actual, legacy, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreScalesGenericRoleDiversityByWeakBaseNoveltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generic-role-diversity-novelty"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-b-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-generator-1", "shared-gn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-near-b-1", "shared-na-nb")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-a-1", "shared-na-nb")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.2},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full bilateral coverage for generic role-diversity novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	legacyRoleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	expectedComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, expectedComp, prior, coord, lockPenalty)
	legacyComp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*legacyRoleDiversitySignal)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, legacyComp, prior, coord, lockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected generic role-diversity signal to scale by weak base novelty without far reviewer, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected weak base novelty generic role-diversity scaling to stay below legacy uplift, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreAddsTopologyAwareLockPenaltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-topology-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b", "agent-near-c")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-near-c-1", "shared-nb-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-c", "near-c-near-b-1", "shared-nb-c")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-c", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.5},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 2 {
		t.Fatalf("expected full bilateral coverage with disconnected two-edge overlap topology for no-FAR lock topology regression, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := 1.0 - float64(2-1)/float64(len(members)-1)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.10
	coord += 0.12 * float64(2-1) / float64(len(members)-1)
	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	legacyLockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	legacyLockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak topology to add bounded no-FAR lock penalty, got actual=%f expected=%f topology_factor=%f", actual, expected, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected weak-topology no-FAR lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f topology_factor=%f", actual, legacy, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreAddsTopologyAwareLockPenaltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-topology-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-far", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-near-a-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-generator-1", "shared-gn-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-near-b-1", "shared-fn-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-far-1", "shared-fn-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.3},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.7, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	overlapEdges := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
			if distance < 1.0 {
				overlapEdges++
			}
		}
	}
	if totalPairs != 6 || evidencePairs != 6 || overlapEdges != 2 {
		t.Fatalf("expected full bilateral coverage with disconnected two-edge overlap topology for FAR lock topology regression, total_pairs=%d evidence_pairs=%d overlap_edges=%d", totalPairs, evidencePairs, overlapEdges)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	overlapTopologyFactor := 1.0 - float64(2-1)/float64(len(members)-1)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, overlapTopologyFactor, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.12 * float64(2-1) / float64(len(members)-1)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, overlapTopologyFactor, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, overlapTopologyFactor, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, overlapTopologyFactor, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyTopologySurcharge(overlapTopologyFactor, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	legacyLockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak topology to add bounded FAR lock penalty, got actual=%f expected=%f topology_factor=%f", actual, expected, overlapTopologyFactor)
	}
	if actual >= legacy {
		t.Fatalf("expected weak-topology FAR lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f topology_factor=%f", actual, legacy, overlapTopologyFactor)
	}
}

func TestCoalitionSynergyScoreAddsEvidenceAwareLockPenaltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-coverage-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-a", "near-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-b", "near-a-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for no-FAR lock penalty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)
	coord += 0.10

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected sparse evidence to add bounded no-FAR lock penalty, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected sparse no-FAR lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreAddsEvidenceAwareLockPenaltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-coverage-with-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.6},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 1 {
		t.Fatalf("expected single-pair evidence coverage for FAR-reviewer lock penalty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += 0.20 * (1.0 - evidenceCoverage)
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	legacyLockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected sparse evidence to add bounded FAR-reviewer lock penalty, got actual=%f expected=%f evidence_coverage=%f", actual, expected, evidenceCoverage)
	}
	if actual >= legacy {
		t.Fatalf("expected sparse FAR-reviewer lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f evidence_coverage=%f", actual, legacy, evidenceCoverage)
	}
}

func TestCoalitionSynergyScoreAddsComplementarityAwareLockPenaltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-complementarity-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-shared-b", "shared-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for no-FAR lock penalty complementarity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance >= 1.0 {
		t.Fatalf("expected overlap-heavy trio to keep weak pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	legacyLockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to add bounded no-FAR lock penalty, got actual=%f expected=%f pairwise_distance=%f", actual, expected, pairwiseDistance)
	}
	if actual >= legacy {
		t.Fatalf("expected weak no-FAR lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f pairwise_distance=%f", actual, legacy, pairwiseDistance)
	}
}

func TestCoalitionSynergyScoreAddsComplementarityAwareLockPenaltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-complementarity-with-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-shared-b", "shared-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.3},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.4},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.6},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for FAR-reviewer lock penalty complementarity regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance >= 1.0 {
		t.Fatalf("expected overlap-heavy FAR-reviewer trio to keep weak pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, coalitionBaseNoveltySignal(members), coalitionRoleDiversity(members), len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := normalizeHistoricalCoalitionPrior(0)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), coalitionHasFarReviewer(members))
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, coalitionBaseNoveltySignal(members), roleDiversity, len(members), coalitionHasFarReviewer(members), 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	legacyLockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak pairwise distance to add bounded FAR-reviewer lock penalty, got actual=%f expected=%f pairwise_distance=%f", actual, expected, pairwiseDistance)
	}
	if actual >= legacy {
		t.Fatalf("expected weak FAR-reviewer lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f pairwise_distance=%f", actual, legacy, pairwiseDistance)
	}
}

func TestCoalitionSynergyScoreAddsNoveltyAwareLockPenaltyWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-novelty-no-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near-a", "agent-near-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-a", "near-a-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-a", "near-a-only-b", "near-a-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-only-a", "near-b-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near-b", "near-b-only-b", "near-b-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near-a", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-near-b", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for no-FAR lock novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance != 1.0 {
		t.Fatalf("expected fully disjoint trio to keep full pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, novelty, roleDiversity, len(members), false, 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), false)
	roleDiversitySignal *= coalitionGenericRoleDiversityNoveltyRetention(novelty, len(members))
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, false)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, false)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), false)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), false)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, novelty, roleDiversity, len(members), false, 0, 0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), false)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity) + 0.35
	legacyLockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), false)
	legacyLockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), false)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak base novelty to add bounded no-FAR lock penalty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected novelty-aware no-FAR lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScoreAddsNoveltyAwareLockPenaltyWithFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-lock-novelty-with-far"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-only-a", "near-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-only-b", "near-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	members := []WorkspaceCoalitionMember{
		{AgentID: "agent-generator", Role: "GENERATOR", FitScore: 0.9, NoveltyScore: 0.1},
		{AgentID: "agent-near", Role: "NEAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
		{AgentID: "agent-far", Role: "FAR_REVIEWER", FitScore: 0.8, NoveltyScore: 0.8},
	}

	actual, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, members, currentEpoch)
	if err != nil {
		t.Fatalf("calculate coalition synergy score: %v", err)
	}

	totalPairs := 0
	evidencePairs := 0
	distanceSum := 0.0
	for idx := 0; idx < len(members); idx++ {
		for jdx := idx + 1; jdx < len(members); jdx++ {
			totalPairs++
			distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, members[idx].AgentID, members[jdx].AgentID)
			if err != nil {
				t.Fatalf("calculate pairwise distance stats: %v", err)
			}
			if !hasEvidence {
				continue
			}
			evidencePairs++
			distanceSum += clampCoalitionSignal(distance)
		}
	}
	if totalPairs != 3 || evidencePairs != 3 {
		t.Fatalf("expected full pairwise evidence coverage for FAR-reviewer lock novelty regression, total_pairs=%d evidence_pairs=%d", totalPairs, evidencePairs)
	}

	evidenceCoverage := float64(evidencePairs) / float64(totalPairs)
	pairwiseDistance := distanceSum / float64(evidencePairs)
	if pairwiseDistance != 1.0 {
		t.Fatalf("expected fully disjoint FAR-reviewer trio to keep full pairwise complementarity, got %f", pairwiseDistance)
	}

	goal := coalitionGoalSignal(members)
	novelty := coalitionBaseNoveltySignal(members)
	roleDiversity := coalitionRoleDiversity(members)
	goalScore := coalitionExpectedGoalScore(goal, evidenceCoverage, pairwiseDistance, 1.0, novelty, roleDiversity, len(members), true, 0, 0, 0)
	roleDiversitySignal := coalitionReviewerDiversitySignal(roleDiversity, evidenceCoverage, pairwiseDistance, goal, 1.0, 0, 0, len(members), true)
	roleDiversitySignal *= coalitionReviewerDiversityNoveltyRetention(novelty, len(members), true)
	prior := coalitionHistoricalPriorSignal(0, novelty, evidenceCoverage, 1.0, pairwiseDistance, goal, 0, 0, 0)
	prior *= coalitionHistoricalPriorRoleDiversityRetention(roleDiversity, true)
	prior *= coalitionHistoricalPriorRoleDiversityNoveltyRetention(novelty, true)
	coord := 0.35 + 0.22*float64(len(members)) + 0.06*float64(len(members)*len(members))
	coord += coalitionCoordinationGoalPenalty(goal, len(members))
	coord += coalitionCoordinationComplementarityPenalty(pairwiseDistance, len(members))
	coord += coalitionCoordinationRoleDiversityPenalty(roleDiversity, len(members), true)
	coord += coalitionCoordinationNoveltyPenalty(novelty, len(members))
	coord += coalitionCoordinationLockPenalty(0, 0, 0, len(members))
	coord += 0.10 * (1.0 - evidenceCoverage)

	noveltySignal := coalitionNoveltySignal(novelty, goal, evidenceCoverage, pairwiseDistance, 1.0, len(members))
	noveltySignal *= coalitionNoveltyRoleDiversityRetention(roleDiversity, len(members), true)
	pairwiseDistanceSignal := coalitionExpectedPairwiseDistanceSignal(pairwiseDistance, evidenceCoverage, goal, 1.0, novelty, roleDiversity, len(members), true, 0, 0, 0)
	farReviewerBonus := coalitionFarReviewerBonusSignal(evidenceCoverage, pairwiseDistance, goal, 1.0, len(members))
	farReviewerBonus *= coalitionFarReviewerBonusRoleDiversityRetention(roleDiversity)
	farReviewerBonus *= coalitionFarReviewerBonusNoveltyRetention(novelty)
	farReviewerBonus *= coalitionFarReviewerBonusLockRetention(0, 0)
	comp := clampCoalitionSignal(0.55*noveltySignal + 0.35*pairwiseDistanceSignal + 0.10*roleDiversitySignal + farReviewerBonus)

	lockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	lockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	lockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	lockPenalty += coalitionLockPenaltyNoveltySurcharge(novelty, len(members), true)
	expected := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, lockPenalty)

	legacyLockPenalty := 1.0 + 0.15*(1.0-roleDiversity)
	legacyLockPenalty += coalitionLockPenaltyEvidenceSurcharge(evidenceCoverage, len(members), true)
	legacyLockPenalty += coalitionLockPenaltyComplementaritySurcharge(pairwiseDistance, len(members), true)
	legacy := CalculateCoalitionSynergyFromSignals(goalScore, comp, prior, coord, legacyLockPenalty)

	if math.Abs(actual-expected) > 1e-9 {
		t.Fatalf("expected weak base novelty to add bounded FAR-reviewer lock penalty, got actual=%f expected=%f novelty=%f", actual, expected, novelty)
	}
	if actual >= legacy {
		t.Fatalf("expected novelty-aware FAR-reviewer lock penalty to stay below legacy fairness penalty, actual=%f legacy=%f novelty=%f", actual, legacy, novelty)
	}
}

func TestCoalitionSynergyScorePenalizesActiveGeneratorLockWithoutFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-generator-lock"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-near")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:signals:generator-lock"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only", "g-only")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-a", "shared-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-b", "shared-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-shared-c", "shared-c")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-near", "near-only", "near-only")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "penalize active generator lock without far reviewer")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-near", 0.8, 0.4); err != nil {
		t.Fatalf("add near reviewer: %v", err)
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	for _, forced := range []struct {
		agentID string
		role    string
	}{
		{agentID: "agent-generator", role: "GENERATOR"},
		{agentID: "agent-near", role: "NEAR_REVIEWER"},
	} {
		if _, err := store.DB().ExecContext(ctx,
			`UPDATE workspace_coalition_members
			 SET role = ?
			 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			forced.role,
			workspaceID,
			coalition.CoalitionID,
			forced.agentID,
		); err != nil {
			t.Fatalf("force role %s/%s: %v", forced.agentID, forced.role, err)
		}
	}

	lockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load locked coalition members: %v", err)
	}
	if len(lockedMembers) != 2 {
		t.Fatalf("expected two-member coalition after forced role shape, got %+v", lockedMembers)
	}

	rolesByAgent := map[string]string{}
	for _, member := range lockedMembers {
		rolesByAgent[member.AgentID] = member.Role
	}
	if rolesByAgent["agent-generator"] != "GENERATOR" || rolesByAgent["agent-near"] != "NEAR_REVIEWER" {
		t.Fatalf("expected generator/near reviewer role shape, got %+v", rolesByAgent)
	}

	lockedPressure := coalitionActiveGeneratorLockPressure(lockedMembers, currentEpoch)
	if lockedPressure <= 0 {
		t.Fatalf("expected active generator lock pressure on forced coalition shape, members=%+v", lockedMembers)
	}
	lockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, lockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute locked synergy score: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_coalition_members
		 SET min_stay_until_epoch = ?
		 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
		currentEpoch,
		workspaceID,
		coalition.CoalitionID,
		"agent-generator",
	); err != nil {
		t.Fatalf("clear generator min-stay: %v", err)
	}

	unlockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load unlocked coalition members: %v", err)
	}
	unlockedPressure := coalitionActiveGeneratorLockPressure(unlockedMembers, currentEpoch)
	if unlockedPressure != 0 {
		t.Fatalf("expected cleared generator min-stay to remove generator-lock pressure, members=%+v pressure=%f", unlockedMembers, unlockedPressure)
	}

	unlockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, unlockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute unlocked synergy score: %v", err)
	}
	if unlockedScore <= lockedScore {
		t.Fatalf("expected cleared generator min-stay to improve generator-lock-adjusted synergy, locked=%f unlocked=%f", lockedScore, unlockedScore)
	}
}

func TestCoalitionSynergyScorePenalizesActiveFarReviewerLockWithoutNearReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-synergy-far-lock"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-far")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:signals:far-lock"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-a", "generator-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only-b", "generator-only-b")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-a", "far-only-a")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-far", "far-only-b", "far-only-b")

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "penalize active far reviewer lock without near reviewer")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-far", 0.8, 0.8); err != nil {
		t.Fatalf("add far reviewer: %v", err)
	}

	currentEpoch, err := store.currentControlEpoch(ctx, store.DB(), workspaceID)
	if err != nil {
		t.Fatalf("load current epoch: %v", err)
	}

	for _, forced := range []struct {
		agentID string
		role    string
	}{
		{agentID: "agent-generator", role: "GENERATOR"},
		{agentID: "agent-far", role: "FAR_REVIEWER"},
	} {
		if _, err := store.DB().ExecContext(ctx,
			`UPDATE workspace_coalition_members
			 SET role = ?
			 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
			forced.role,
			workspaceID,
			coalition.CoalitionID,
			forced.agentID,
		); err != nil {
			t.Fatalf("force role %s/%s: %v", forced.agentID, forced.role, err)
		}
	}

	lockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load locked coalition members: %v", err)
	}
	if len(lockedMembers) != 2 {
		t.Fatalf("expected two-member coalition after forced role shape, got %+v", lockedMembers)
	}

	rolesByAgent := map[string]string{}
	for _, member := range lockedMembers {
		rolesByAgent[member.AgentID] = member.Role
	}
	if rolesByAgent["agent-generator"] != "GENERATOR" || rolesByAgent["agent-far"] != "FAR_REVIEWER" {
		t.Fatalf("expected generator/far reviewer role shape, got %+v", rolesByAgent)
	}

	lockedPressure := coalitionActiveFarReviewerLockPressure(lockedMembers, currentEpoch)
	if lockedPressure <= 0 {
		t.Fatalf("expected active far-reviewer lock pressure on forced coalition shape, members=%+v", lockedMembers)
	}
	lockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, lockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute locked synergy score: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_coalition_members
		 SET min_stay_until_epoch = ?
		 WHERE workspace_id = ? AND coalition_id = ? AND agent_id = ?`,
		currentEpoch,
		workspaceID,
		coalition.CoalitionID,
		"agent-far",
	); err != nil {
		t.Fatalf("clear far reviewer min-stay: %v", err)
	}

	unlockedMembers, err := store.loadCoalitionMembers(ctx, store.DB(), workspaceID, coalition.CoalitionID)
	if err != nil {
		t.Fatalf("load unlocked coalition members: %v", err)
	}
	unlockedPressure := coalitionActiveFarReviewerLockPressure(unlockedMembers, currentEpoch)
	if unlockedPressure != 0 {
		t.Fatalf("expected cleared far-reviewer min-stay to remove far-reviewer lock pressure, members=%+v pressure=%f", unlockedMembers, unlockedPressure)
	}
	unlockedScore, err := store.calculateCoalitionSynergyScore(ctx, store.DB(), workspaceID, 0, unlockedMembers, currentEpoch)
	if err != nil {
		t.Fatalf("recompute unlocked synergy score: %v", err)
	}
	if unlockedScore <= lockedScore {
		t.Fatalf("expected cleared far-reviewer min-stay to improve far-reviewer-lock-adjusted synergy, locked=%f unlocked=%f", lockedScore, unlockedScore)
	}
}

func TestAddCoalitionMemberClampsScoringInputs(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-score-clamp"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:score-clamp"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "clamp coalition score inputs")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-a", 2.5, -1.0); err != nil {
		t.Fatalf("add out-of-range generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-b", -0.2, 3.0); err != nil {
		t.Fatalf("add out-of-range reviewer: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition after clamped writes: %v", err)
	}
	if current == nil || len(current.Members) != 2 {
		t.Fatalf("expected two coalition members after clamped writes, got %+v", current)
	}
	for _, member := range current.Members {
		if member.FitScore < 0 || member.FitScore > 1 {
			t.Fatalf("expected fit score clamp for %+v", member)
		}
		if member.NoveltyScore < 0 || member.NoveltyScore > 1 {
			t.Fatalf("expected novelty score clamp for %+v", member)
		}
	}
}

func TestCoalitionRoleNormalizationRequiresEvidenceForFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-far-evidence"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-reviewer")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:far-evidence"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "far reviewer should require evidence")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-reviewer", 0.8, 0.9); err != nil {
		t.Fatalf("add reviewer without overlap evidence: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}
	if rolesByAgent["agent-reviewer"] != "NEAR_REVIEWER" {
		t.Fatalf("expected missing overlap evidence to stay near-reviewer, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionRoleNormalizationRequiresBilateralEvidenceForFarReviewer(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-far-bilateral-evidence"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-generator", "agent-reviewer")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tensionID := "tension:coalition:far-bilateral-evidence"
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		WorkspaceID:    workspaceID,
		TensionID:      tensionID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-generator", "generator-only", "generator-only")

	distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStats(ctx, store.DB(), workspaceID, "agent-generator", "agent-reviewer")
	if err != nil {
		t.Fatalf("calculate pairwise distance stats with one-sided memory context: %v", err)
	}
	if hasEvidence {
		t.Fatalf("expected one-sided memory footprint to stay evidence-free, got distance=%f", distance)
	}
	if distance != 0 {
		t.Fatalf("expected one-sided memory footprint to stay neutral, got %f", distance)
	}

	coalition, err := store.CreateCoalition(ctx, workspaceID, tensionID, "far reviewer should require bilateral evidence")
	if err != nil {
		t.Fatalf("create coalition: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-generator", 0.9, 0.3); err != nil {
		t.Fatalf("add generator: %v", err)
	}
	if err := store.AddCoalitionMember(ctx, workspaceID, coalition.CoalitionID, "agent-reviewer", 0.8, 0.9); err != nil {
		t.Fatalf("add reviewer with one-sided memory footprint: %v", err)
	}

	current, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get coalition: %v", err)
	}
	rolesByAgent := make(map[string]string, len(current.Members))
	for _, member := range current.Members {
		rolesByAgent[member.AgentID] = member.Role
	}
	if rolesByAgent["agent-reviewer"] != "NEAR_REVIEWER" {
		t.Fatalf("expected one-sided memory footprint to stay near-reviewer, got roles %+v", rolesByAgent)
	}
}

func TestCoalitionPairwiseDistanceReadRefReturnsNoEvidenceWithoutMemoryContext(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-distance-no-evidence"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, "agent-a", "agent-b")
	if err != nil {
		t.Fatalf("calculate pairwise distance read-ref: %v", err)
	}
	if hasEvidence {
		t.Fatalf("expected missing memory context to report no evidence, got distance=%f", distance)
	}
	if distance != 0 {
		t.Fatalf("expected zero distance when evidence is missing, got %f", distance)
	}
}

func TestCoalitionPairwiseDistanceReadRefRequiresBilateralMemoryContext(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-distance-bilateral-evidence"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-a", "a-only", "a-only")

	distance, hasEvidence, err := store.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, "agent-a", "agent-b")
	if err != nil {
		t.Fatalf("calculate read-ref distance with one-sided memory context: %v", err)
	}
	if hasEvidence {
		t.Fatalf("expected one-sided memory context to report no bilateral evidence, got distance=%f", distance)
	}
	if distance != 0 {
		t.Fatalf("expected one-sided memory context to stay neutral, got %f", distance)
	}
}

func TestCoalitionPairwiseDistanceReadRefUsesSharedSourceEvidenceAcrossDistinctMemoryIDs(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-coalition-distance-shared-source"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b", "agent-c")
	ensureTensionOverlayTables(t, ctx, store)

	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-a", "memory-a", "shared-source")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-b", "memory-b", "shared-source")
	writeCoalitionRoleMemory(t, ctx, store, workspaceID, "agent-c", "memory-c", "disjoint-source")

	sharedDistance, hasSharedEvidence, err := store.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, "agent-a", "agent-b")
	if err != nil {
		t.Fatalf("calculate read-ref distance with shared source evidence: %v", err)
	}
	if !hasSharedEvidence {
		t.Fatalf("expected shared source evidence across distinct memory ids to count as bilateral evidence")
	}
	disjointDistance, hasDisjointEvidence, err := store.calculateCoalitionPairwiseDistanceStatsReadRef(ctx, workspaceID, "agent-a", "agent-c")
	if err != nil {
		t.Fatalf("calculate read-ref distance with disjoint source evidence: %v", err)
	}
	if !hasDisjointEvidence {
		t.Fatalf("expected disjoint pair to still report bilateral evidence once both sides have memory context")
	}
	if sharedDistance >= disjointDistance {
		t.Fatalf("expected shared source evidence to reduce pairwise distance across distinct memory ids, shared=%f disjoint=%f", sharedDistance, disjointDistance)
	}
}

func TestRefreshMetaTensionsSkipsMetaTensionCycles(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-meta-guard"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	ensureTensionOverlayTables(t, ctx, store)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, rec := range []tensionRecordFixture{
		{TensionID: "M1", WorkspaceID: workspaceID, TensionType: "meta-tension", LifecycleState: tensionLifecycleActive, CreatedAt: now, UpdatedAt: now},
		{TensionID: "M2", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, CreatedAt: now, UpdatedAt: now},
		{TensionID: "M3", WorkspaceID: workspaceID, TensionType: "gap", LifecycleState: tensionLifecycleActive, CreatedAt: now, UpdatedAt: now},
	} {
		insertTensionRecordFixture(t, ctx, store, rec)
	}

	if err := store.AddTensionDependency(ctx, workspaceID, "M1", "M2", "BLOCKS"); err != nil {
		t.Fatalf("add dependency M1->M2: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "M2", "M3", "BLOCKS"); err != nil {
		t.Fatalf("add dependency M2->M3: %v", err)
	}
	if err := store.AddTensionDependency(ctx, workspaceID, "M3", "M1", "BLOCKS"); err != nil {
		t.Fatalf("add dependency M3->M1: %v", err)
	}

	if err := store.RefreshMetaTensions(ctx, workspaceID); err != nil {
		t.Fatalf("refresh meta tensions: %v", err)
	}

	tensions, err := store.ListTensions(ctx, TensionFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}

	metaCount := 0
	for _, tension := range tensions {
		if tension.TensionType == "meta-tension" {
			metaCount++
		}
	}
	if metaCount != 1 {
		t.Fatalf("expected the existing meta tension to remain singular, got %d", metaCount)
	}

	for _, tensionID := range []string{"M1", "M2", "M3"} {
		detail, err := store.GetTension(ctx, workspaceID, tensionID)
		if err != nil {
			t.Fatalf("get tension %s: %v", tensionID, err)
		}
		if detail.Tension.LifecycleState != tensionLifecycleActive {
			t.Fatalf("expected %s to remain ACTIVE, got %s", tensionID, detail.Tension.LifecycleState)
		}
	}
}

func writeCoalitionRoleMemory(t *testing.T, ctx context.Context, store *Store, workspaceID, agentID, memoryID, sourceID string) {
	t.Helper()

	if _, err := store.WriteMemoryNode(ctx, MemoryNodeWriteInput{
		WorkspaceID: workspaceID,
		MemoryID:    memoryID,
		MemoryType:  "ENTITY",
		Body:        "coalition role overlap " + sourceID,
		AgentID:     agentID,
		SourceKind:  "coalition-role",
		SourceID:    sourceID,
	}); err != nil {
		t.Fatalf("write coalition role memory %s/%s: %v", agentID, sourceID, err)
	}
}
