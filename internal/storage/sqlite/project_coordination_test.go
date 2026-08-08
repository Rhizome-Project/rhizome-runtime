package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectCoordinationProfilePhaseAndGates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-coordination"
		projectID   = "project-coordination"
		actorID     = "lead-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{actorID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Coordination",
		Description: "Build the shared product without isolated parallel implementations.",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile: %v", err)
	}
	if profile.CurrentPhase != sqlite.ProjectPhaseIntake || profile.Goal == "" || profile.RepoStatus != sqlite.ProjectRepoStatusNotRequired {
		t.Fatalf("unexpected default profile %+v", profile)
	}
	gates, err := store.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get default gate status: %v", err)
	}
	if gates.ImplementationReady || gates.OverallState != sqlite.ProjectGateStateBlocked {
		t.Fatalf("expected implementation gates to start closed, got %+v", gates)
	}

	designDocID := "doc.design.v1"
	implementationPlanDocID := "doc.plan.v1"
	repoRequired := true
	repoStatus := sqlite.ProjectRepoStatusReady
	repoURL := "git@github.com:ExampleOrg/project-coordination.git"
	repoDefaultBranch := "main"
	updatedProfile, profileEvent, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		RepoRequired:            &repoRequired,
		RepoStatus:              &repoStatus,
		RepoURL:                 &repoURL,
		RepoDefaultBranch:       &repoDefaultBranch,
		ActorID:                 actorID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:    "project.profile.update",
	})
	if err != nil {
		t.Fatalf("upsert project profile: %v", err)
	}
	if profileEvent.EventType != "project.profile.updated" || updatedProfile.DesignDocID != designDocID || updatedProfile.RepoStatus != sqlite.ProjectRepoStatusReady {
		t.Fatalf("unexpected profile update result profile=%+v event=%+v", updatedProfile, profileEvent)
	}
	gates, err = store.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get updated gate status: %v", err)
	}
	if gates.ImplementationReady || gates.OverallState != sqlite.ProjectGateStateBlocked {
		t.Fatalf("expected implementation gates to remain blocked until lead and phase are ready, got %+v", gates)
	}

	lead, leadEvent, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "Lead owns phase movement for the project.",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if lead.RoleType != sqlite.ProjectRoleStrategicLead || leadEvent.EventType != "project.lead.claimed" {
		t.Fatalf("unexpected lead claim role=%+v event=%+v", lead, leadEvent)
	}

	phaseProfile, history, phaseEvent, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Design and implementation plan accepted.",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.phase.transition",
	})
	if err != nil {
		t.Fatalf("transition project phase: %v", err)
	}
	if phaseProfile.CurrentPhase != sqlite.ProjectPhaseImplementation || history.FromPhase != sqlite.ProjectPhaseIntake || phaseEvent.EventType != "project.phase.transitioned" {
		t.Fatalf("unexpected phase transition profile=%+v history=%+v event=%+v", phaseProfile, history, phaseEvent)
	}
	gates, err = store.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get implementation gate status: %v", err)
	}
	if !gates.ImplementationReady || gates.OverallState != "PARTIAL" {
		t.Fatalf("expected implementation gates ready with optional later gates partial, got %+v", gates)
	}

	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-coordination-planning", "planning", false)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-coordination-implementation", "implementation", true)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-coordination-planning",
		AgentID:     actorID,
		Summary:     "claimed planning slice for coordination snapshot visibility",
	}); err != nil {
		t.Fatalf("claim coordination planning task: %v", err)
	}
	const taskUpdatedAt = "2026-05-02T00:00:00Z"
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE tasks
   SET description = ?, priority = ?, tags_json = ?, updated_at = ?
 WHERE task_id = ?`,
		"Patch queue decision follow-up must stay visible in project coordination.",
		"critical",
		`["patch-queue","coordination"]`,
		taskUpdatedAt,
		"task-coordination-planning"); err != nil {
		t.Fatalf("update coordination planning task metadata: %v", err)
	}

	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project coordination: %v", err)
	}
	if coordination.SnapshotAt == "" || coordination.CoordinationVersion == "" {
		t.Fatalf("expected coordination snapshot/version envelope, got %+v", coordination)
	}
	if coordination.Profile.CurrentPhase != sqlite.ProjectPhaseImplementation || !coordination.GateStatus.ImplementationReady {
		t.Fatalf("unexpected coordination summary %+v", coordination)
	}
	if coordination.StrategicLead == nil || coordination.StrategicLead.AgentID != actorID {
		t.Fatalf("expected active strategic lead in coordination summary, got %+v", coordination)
	}
	if len(coordination.Tasks) != 2 || coordination.OpenTaskCount != 2 {
		t.Fatalf("expected project tasks in coordination snapshot, got tasks=%+v open=%d", coordination.Tasks, coordination.OpenTaskCount)
	}
	if coordination.TaskCountsByLane["planning"] != 1 || coordination.TaskCountsByLane["implementation"] != 1 {
		t.Fatalf("expected lane task counts in coordination snapshot, got %+v", coordination.TaskCountsByLane)
	}
	if coordination.TaskCountsByStatus["PENDING"] != 1 || coordination.TaskCountsByStatus["RUNNING"] != 1 {
		t.Fatalf("expected status task counts in coordination snapshot, got %+v", coordination.TaskCountsByStatus)
	}
	if !strings.Contains(coordination.CoordinationVersion, "|tasks:") {
		t.Fatalf("expected coordination version to include task freshness, got %q", coordination.CoordinationVersion)
	}
	claimedTask := findCoordinationTaskForTest(t, coordination.Tasks, "task-coordination-planning")
	if claimedTask.ClaimAgentID == nil || *claimedTask.ClaimAgentID != actorID || claimedTask.ClaimStatus == nil || *claimedTask.ClaimStatus != "CLAIMED" {
		t.Fatalf("expected claim state in project coordination task snapshot, got %+v", claimedTask)
	}
	if claimedTask.UpdatedAt != taskUpdatedAt || claimedTask.Priority != "critical" || len(claimedTask.Tags) != 2 || claimedTask.Tags[0] != "patch-queue" {
		t.Fatalf("expected task metadata freshness fields in project coordination snapshot, got %+v", claimedTask)
	}
}

func TestProjectCoordinationAcceptsCaseVariantProjectID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-case-alias"
		projectID   = "project-signal01-checkpoint-fixture-20260602T235249Z"
		leadID      = "alpha"
	)
	projectAlias := strings.ToLower(projectID)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Signal Fixture",
		Description: "Canonical project id keeps timestamp casing.",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "project.signal.fixture.design"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		DesignDocID:           &designDocID,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.profile.update",
	}); err != nil {
		t.Fatalf("update profile: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim lead: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-case-alias-integration", "integration", true)

	project, err := store.GetProject(ctx, workspaceID, projectAlias)
	if err != nil {
		t.Fatalf("get project by case alias: %v", err)
	}
	if project.ProjectID != projectID {
		t.Fatalf("project id = %q, want canonical %q", project.ProjectID, projectID)
	}
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectAlias)
	if err != nil {
		t.Fatalf("get profile by case alias: %v", err)
	}
	if profile.ProjectID != projectID || profile.DesignDocID != designDocID {
		t.Fatalf("profile did not resolve canonical project, got %+v", profile)
	}
	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectAlias)
	if err != nil {
		t.Fatalf("get coordination by case alias: %v", err)
	}
	if coordination.Project.ProjectID != projectID || coordination.Profile.ProjectID != projectID {
		t.Fatalf("coordination did not preserve canonical project id: %+v", coordination)
	}
	if coordination.StrategicLead == nil || coordination.StrategicLead.AgentID != leadID {
		t.Fatalf("case-alias coordination lost strategic lead: %+v", coordination.StrategicLead)
	}
	activeLead, ok, err := store.GetActiveProjectStrategicLead(ctx, workspaceID, projectAlias)
	if err != nil {
		t.Fatalf("get active lead by case alias: %v", err)
	}
	if !ok || activeLead.ProjectID != projectID || activeLead.AgentID != leadID {
		t.Fatalf("active lead did not resolve canonical project id: ok=%v lead=%+v", ok, activeLead)
	}
	if len(coordination.Tasks) != 1 || coordination.Tasks[0].ProjectID != projectID {
		t.Fatalf("case-alias coordination lost canonical project tasks: %+v", coordination.Tasks)
	}
}

func TestProjectCoordinationReconcilesReadyCanonicalRepoWhenProfileIsStale(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-repo-profile-reconcile"
		projectID   = "project-repo-profile-reconcile"
		agentID     = "lead-reconcile"
		taskID      = "task-reconcile-implementation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Repo Profile Reconcile",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "doc.reconcile.design"
	implementationPlanDocID := "doc.reconcile.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 agentID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("seed profile docs: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim lead: %v", err)
	}
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, agentID, agentID, `{"paths":["app/**"]}`)
	repo, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-ready",
		RemoteURL:             "file:///tmp/repo-profile-reconcile.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.repository.upsert",
	})
	if err != nil {
		t.Fatalf("upsert ready repository: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Ready to implement.",
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_profiles
   SET repo_status = ?, repo_url = ?, updated_by = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ?`,
		sqlite.ProjectRepoStatusMissing, repo.RemoteURL, "stale-bootstrap", "2026-05-04T14:42:33Z", workspaceID, projectID); err != nil {
		t.Fatalf("make profile stale: %v", err)
	}

	gates, err := store.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get gates: %v", err)
	}
	repoGate := findProjectGateForTest(t, gates.Gates, "repo_ready_or_not_required")
	if repoGate.State != sqlite.ProjectGateStateSatisfied {
		t.Fatalf("expected gate status to trust canonical ready repository, got %+v", repoGate)
	}
	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get coordination: %v", err)
	}
	if coordination.Profile.RepoStatus != sqlite.ProjectRepoStatusReady || coordination.Profile.RepoURL != repo.RemoteURL {
		t.Fatalf("expected coordination profile to reconcile ready repo, got profile=%+v repo=%+v", coordination.Profile, repo)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claim without checkout should still be gated by canonical repo",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) {
		t.Fatalf("expected canonical READY repo to require implementation claim bindings despite stale profile, got %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		Summary:          "trust-first claim records advisory admission gaps and keeps moving",
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "requires branch_id, checkout_id, and write_scope_json") {
		t.Fatalf("expected trust-first project claim admission to keep canonical repo bindings mandatory, got %v", err)
	}
}

func TestProjectGateManualOverrideEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gate-override"
		projectID   = "project-gate-override"
		actorID     = "lead-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{actorID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Gate Override",
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	status, event, err := store.UpsertProjectGateStatusWithEvent(ctx, sqlite.ProjectGateStatusUpdateInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		GateKey:               "design_doc_ready",
		State:                 sqlite.ProjectGateStateWaived,
		Summary:               "Operator waived design doc for a tiny non-code slice.",
		EvidenceRef:           "decision:tiny-slice-waiver",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.gate.update", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.gate.update",
	})
	if err != nil {
		t.Fatalf("upsert project gate: %v", err)
	}
	if event.EventType != "project.gate.updated" {
		t.Fatalf("unexpected gate event %+v", event)
	}
	gate := findProjectGateForTest(t, status.Gates, "design_doc_ready")
	if gate.State != sqlite.ProjectGateStateWaived || gate.Source != "manual" || !gate.Required {
		t.Fatalf("unexpected manual gate override %+v", gate)
	}
}

func TestGetAgentWorkNextProjectImplementationGate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gated-agent-work"
		projectID   = "project-gated-agent-work"
		taskID      = "task-gated-implementation"
		agentID     = "implementation-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Gated Implementation",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)

	blocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get gated work next: %v", err)
	}
	if blocked.HasWork || blocked.Reason != "project_gate_closed" || blocked.ProjectID != projectID {
		t.Fatalf("expected project gate closure, got %+v", blocked)
	}
	if blocked.Packet == nil || blocked.Packet.Gate == nil || blocked.Packet.Gate.GateType != "project_implementation_gate" {
		t.Fatalf("expected project gate packet, got %+v", blocked.Packet)
	}

	trustFirst, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get trust-first gated work next: %v", err)
	}
	if trustFirst.HasWork || trustFirst.Reason != "project_gate_closed" || trustFirst.ProjectID != projectID || !trustFirst.RequiresProjectGate {
		t.Fatalf("trust-first must keep implementation gate hard-closed, got %+v", trustFirst)
	}

	designDocID := "doc.design.ready"
	implementationPlanDocID := "doc.plan.ready"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 agentID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open implementation gates: %v", err)
	}

	stillBlocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get pre-phase work next: %v", err)
	}
	if stillBlocked.HasWork || stillBlocked.Reason != "project_gate_closed" || stillBlocked.Packet == nil || stillBlocked.Packet.Gate == nil {
		t.Fatalf("expected docs-only project to remain gate-blocked, got %+v", stillBlocked)
	}

	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim implementation lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Implementation gates are satisfied.",
		ActorID:               agentID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation phase: %v", err)
	}

	next, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get opened work next: %v", err)
	}
	if !next.HasWork || next.Reason != "next_pending" || next.Task == nil || next.Task.TaskID != taskID {
		t.Fatalf("expected gated task after profile readiness, got %+v", next)
	}
}

func TestGetAgentWorkNextProjectImplementationWithScopedRolesRequiresTargetedSwitch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-targeted-delegation"
		projectID   = "project-targeted-delegation"
		leadID      = "alpha"
		uiAgentID   = "gamma"
		coreAgentID = "delta"
		uiTaskID    = "task-ui-lane"
		coreTaskID  = "task-core-lane"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, uiAgentID, coreAgentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Targeted Delegation",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "doc.targeted.design"
	implementationPlanDocID := "doc.targeted.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open implementation gates: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim implementation lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Implementation gates are satisfied.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation phase: %v", err)
	}
	for _, role := range []struct {
		agentID string
		scope   string
	}{
		{uiAgentID, `{"paths":["package.json","src/App.*","src/components/**","src/styles/**"]}`},
		{coreAgentID, `{"paths":["src/core/**","src/lib/**","tests/**"]}`},
	} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               role.agentID,
			RoleType:              sqlite.ProjectRoleImplementer,
			WriteScopeJSON:        role.scope,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign implementer role to %s: %v", role.agentID, err)
		}
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, uiTaskID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, coreTaskID)

	generic, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       coreAgentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get generic scoped project work: %v", err)
	}
	if generic.HasWork || generic.Reason != "project_targeted_delegation_required" || generic.ProjectID != projectID {
		t.Fatalf("generic project implementation task should wait for targeted delegation, got %+v", generic)
	}
	if generic.Packet == nil || generic.Packet.Gate == nil || generic.Packet.Gate.GateType != "project_role_targeted_delegation" {
		t.Fatalf("expected targeted delegation packet, got %+v", generic.Packet)
	}

	trustFirstGeneric, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          coreAgentID,
		IncludePacket:    true,
		CoordinationMode: "trust_first",
	})
	if err != nil {
		t.Fatalf("get trust-first generic scoped project work: %v", err)
	}
	if !trustFirstGeneric.HasWork || trustFirstGeneric.Task == nil ||
		(trustFirstGeneric.Task.TaskID != uiTaskID && trustFirstGeneric.Task.TaskID != coreTaskID) {
		t.Fatalf("trust-first should treat targeted delegation as advisory and select useful work, got %+v", trustFirstGeneric)
	}

	delegated, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         coreAgentID,
		Trigger:         "runtime_switch_task",
		CandidateTaskID: coreTaskID,
	})
	if err != nil {
		t.Fatalf("get targeted scoped project work: %v", err)
	}
	if !delegated.HasWork || delegated.Task == nil || delegated.Task.TaskID != coreTaskID || delegated.Trigger != "runtime_switch_task" {
		t.Fatalf("targeted runtime switch should select delegated project task, got %+v", delegated)
	}
}

func TestGetAgentWorkNextScopedImplementerCanSelfSelectCoveredSemanticImplementationTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-covered-semantic-implementation"
		projectID   = "project-covered-semantic-implementation"
		leadID      = "alpha"
		agentID     = "gamma"
		taskID      = "task-signal01-rq-lexer-parser"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Covered Semantic Implementation",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "doc.covered-semantic.design"
	implementationPlanDocID := "doc.covered-semantic.plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("open implementation gates: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim implementation lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Implementation gates are satisfied.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation phase: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["internal/lexer/**","internal/token/**","internal/tokens/**","internal/parser/**","internal/ast/**"]}`,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign implementer role: %v", err)
	}
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, taskID)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE tasks
   SET title = 'Implement rq lexer and precedence parser',
       description = 'Build the rq lexer, token stream, recursive-descent parser, AST, and precedence grammar.',
       write_scope_hints_json = '[]',
       task_requirements_json = '{"schema":"task_requirements.v1","required_work_modes":["implementation"],"preferred_skills":["go","lexer","parser"]}'
 WHERE task_id = ?`, taskID); err != nil {
		t.Fatalf("shape semantic implementation task: %v", err)
	}

	next, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get covered semantic implementation work: %v", err)
	}
	if !next.HasWork || next.Task == nil || next.Task.TaskID != taskID || next.Reason != "next_pending" {
		t.Fatalf("covered scoped implementer should self-select semantic implementation task, got %+v", next)
	}
}

func TestGetAgentWorkNextSkipsImplementationCandidateWithBusyWriteScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-claim-scope-busy"
		projectID   = "project-claim-scope-busy"
		repoID      = "repo-claim-scope-busy"
		leadID      = "alpha"
		ownerID     = "beta"
		agentID     = "gamma"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Claim Scope Busy",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	designDocID := "doc.claim-scope.design"
	planDocID := "doc.claim-scope.plan"
	repoStatus := sqlite.ProjectRepoStatusReady
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &planDocID,
		RepoStatus:              &repoStatus,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("upsert project profile: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "file:///tmp/project-claim-scope-busy.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert project repository: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Implementation gates are satisfied.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation phase: %v", err)
	}
	ownerRole, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               "developer",
		ActorType:             "human",
		AgentID:               ownerID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", "developer"),
		PromptContextSurface:  "project.role.assign",
	})
	if err != nil {
		t.Fatalf("assign owner role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               "developer",
		ActorType:             "human",
		AgentID:               agentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        `{"paths":["**"]}`,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", "developer"),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign agent role: %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-active-implementation")
	ownerCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `/tmp/beta/claim-scope-busy`)
	ownerBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            ownerCheckout.CheckoutID,
		AgentID:               ownerID,
		BranchName:            "agent/beta/project/task-active-implementation",
		WriteScopeJSON:        `{"paths":["**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register owner branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-active-implementation",
		AgentID:               ownerID,
		ProjectRoleID:         ownerRole.RoleID,
		RepoID:                repoID,
		CheckoutID:            ownerCheckout.CheckoutID,
		BranchID:              ownerBranch.BranchID,
		WriteScopeJSON:        `{"paths":["**"]}`,
		Summary:               "claim root implementation slice",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-active-implementation", ownerID),
	}); err != nil {
		t.Fatalf("claim owner implementation task: %v", err)
	}
	registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, "branch-claim-scope-verification-ready")

	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, "task-verification", "verification", false)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, "task-overlapping-implementation")

	next, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get work next: %v", err)
	}
	if !next.HasWork || next.Task == nil || next.Task.TaskID != "task-verification" {
		t.Fatalf("expected scheduler to skip busy implementation scope and choose verification work, got %+v", next)
	}
}

func TestProjectClaimRepairTaskReservedForStrategicLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-claim-repair-lead"
		projectID    = "project-claim-repair-lead"
		leadID       = "alpha"
		builderID    = "beta"
		repairTaskID = "task-project-claim-repair-deadbeef"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Repair lead routing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        leadID,
		Bio:            "Builds implementation slices but currently owns project coordination.",
		Specialization: "implementation",
		Tags:           []string{"implementation"},
		Metadata: map[string]any{
			"default_work_mode": "implementation",
		},
	}); err != nil {
		t.Fatalf("upsert lead profile: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       repairTaskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Repair overlapping project claim",
		Description:  "Strategic lead should reconcile write-scope conflict and unblock implementation.",
		TaskKind:     "COORDINATION",
		ProjectLane:  "strategy",
		ProjectID:    projectID,
		Tags:         []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
		TaskTemplate: "integration",
	}, graph); err != nil {
		t.Fatalf("create repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach repair task: %v", err)
	}

	blocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       builderID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get non-lead repair work: %v", err)
	}
	if blocked.HasWork || blocked.Reason != "none_available" || blocked.Packet != nil {
		t.Fatalf("expected non-lead autonomous selection to skip lead-owned repair task, got %+v", blocked)
	}

	triggeredBlocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  repairTaskID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get triggered non-lead repair work: %v", err)
	}
	if triggeredBlocked.HasWork || triggeredBlocked.Reason != "project_claim_repair_lead_required" || triggeredBlocked.Packet == nil {
		t.Fatalf("expected explicit non-lead repair trigger to receive lead-required packet, got %+v", triggeredBlocked)
	}
	if triggeredBlocked.Packet.Gate == nil || triggeredBlocked.Packet.Gate.NeededFrom != "strategic_lead" || triggeredBlocked.Packet.PreferredTransition != "delegate_to_strategic_lead" {
		t.Fatalf("unexpected lead-required packet: %+v", triggeredBlocked.Packet)
	}
	if len(triggeredBlocked.Packet.ContextHints.AnchorTaskIDs) != 1 || triggeredBlocked.Packet.ContextHints.AnchorTaskIDs[0] != repairTaskID {
		t.Fatalf("expected repair task anchor, got %+v", triggeredBlocked.Packet.ContextHints)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		AgentID:     builderID,
		Summary:     "non-lead direct claim should fail",
	}); !errors.Is(err, sqlite.ErrTaskClaimConflict) || !strings.Contains(err.Error(), "active strategic lead") {
		t.Fatalf("expected direct non-lead claim guard, got %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     leadID,
	})
	if err != nil {
		t.Fatalf("get lead repair work: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != repairTaskID {
		t.Fatalf("expected active strategic lead to receive repair task despite implementation profile, got %+v", leadWork)
	}
}

func TestProjectRepositoryRepairTaskReservedForStrategicLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-repo-repair-lead"
		projectID    = "project-repo-repair-lead"
		leadID       = "iota"
		backstopID   = "alpha"
		repairTaskID = "task-register-canonical-repo-ready"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, backstopID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Repository Repair Lead Routing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        backstopID,
		Specialization: "strategy",
		Tags:           []string{"strategy"},
		Metadata: map[string]any{
			"default_work_mode": "strategy",
		},
	}); err != nil {
		t.Fatalf("upsert backstop profile: %v", err)
	}
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        leadID,
		Specialization: "implementation",
		Tags:           []string{"implementation"},
		Metadata: map[string]any{
			"default_work_mode": "implementation",
		},
	}); err != nil {
		t.Fatalf("upsert lead profile: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: workspaceID,
		AgentID:     leadID,
		Status:      "ACTIVE",
		Summary:     "fresh strategic lead owns repository repair",
	}); err != nil {
		t.Fatalf("heartbeat lead: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       repairTaskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Use strategic-lead authority to register Clearpress canonical repository as READY",
		Description:  "Recover after project.repository.upsert failed because project repository mutation requires active strategic lead. Use project_repo_materialize or project_repo_register to register the canonical repository.",
		TaskKind:     "COORDINATION",
		ProjectLane:  "coordination",
		ProjectID:    projectID,
		Tags:         []string{"repo", "strategic-lead", "blocker-unblock"},
		TaskTemplate: "integration",
	}, graph); err != nil {
		t.Fatalf("create repository repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach repository repair task: %v", err)
	}

	blocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       backstopID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get non-lead repository repair work: %v", err)
	}
	if blocked.HasWork || blocked.Reason != "none_available" || blocked.Packet != nil {
		t.Fatalf("expected fresh lead to keep repository repair task from backstop selection, got %+v", blocked)
	}

	triggeredBlocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          backstopID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  repairTaskID,
		CoordinationMode: "trust_first",
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get triggered non-lead repository repair work: %v", err)
	}
	if triggeredBlocked.HasWork || triggeredBlocked.Reason != "project_claim_repair_lead_required" || triggeredBlocked.Packet == nil {
		t.Fatalf("expected explicit non-lead repository repair trigger to receive lead-required packet, got %+v", triggeredBlocked)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		AgentID:     backstopID,
		Summary:     "non-lead direct repository repair claim should fail",
	}); !errors.Is(err, sqlite.ErrTaskClaimConflict) || !strings.Contains(err.Error(), "project repository repair task") {
		t.Fatalf("expected direct non-lead repository repair claim guard, got %v", err)
	}

	leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     leadID,
	})
	if err != nil {
		t.Fatalf("get lead repository repair work: %v", err)
	}
	if !leadWork.HasWork || leadWork.Task == nil || leadWork.Task.TaskID != repairTaskID {
		t.Fatalf("expected active strategic lead to receive repository repair task, got %+v", leadWork)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		AgentID:     leadID,
		Summary:     "lead claims repository repair",
	}); err != nil {
		t.Fatalf("expected active strategic lead to claim repository repair task: %v", err)
	}
}

func TestProjectClaimRepairTaskFallsBackToIntegratorWhenLeadStale(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-claim-repair-stale-lead"
		projectID    = "project-claim-repair-stale-lead"
		leadID       = "alpha"
		integratorID = "zeta"
		repairTaskID = "task-project-claim-repair-stale-lead"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        integratorID,
		Specialization: "integration",
		Tags:           []string{"integrator"},
		Metadata: map[string]any{
			"default_work_mode": "integrator",
		},
	}); err != nil {
		t.Fatalf("upsert integrator profile: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Repair stale lead fallback",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: workspaceID,
		AgentID:     leadID,
		Status:      "ACTIVE",
		Summary:     "fresh strategic lead",
	}); err != nil {
		t.Fatalf("heartbeat lead: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:  workspaceID,
		TaskID:       repairTaskID,
		OwnerUserID:  "developer",
		Priority:     "critical",
		Title:        "Repair overlapping project claim",
		Description:  "Strategic lead should reconcile write-scope conflict and unblock implementation.\n\nConflict:\n- blocked_task_id: task-downstream\n- blocked_agent_id: beta\n- conflict_kind: live_claim_overlap",
		TaskKind:     "COORDINATION",
		ProjectLane:  "strategy",
		ProjectID:    projectID,
		Tags:         []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
		TaskTemplate: "integration",
	}, graph); err != nil {
		t.Fatalf("create repair task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach repair task: %v", err)
	}

	freshLeadResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
	})
	if err != nil {
		t.Fatalf("get integrator with fresh lead: %v", err)
	}
	if freshLeadResult.HasWork || freshLeadResult.Reason != "none_available" {
		t.Fatalf("expected fresh lead to retain repair ownership without blocking integrator autonomy, got %+v", freshLeadResult)
	}

	staleAt := time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?`,
		staleAt, staleAt, workspaceID, leadID,
	); err != nil {
		t.Fatalf("stale lead heartbeat: %v", err)
	}

	staleLeadResult, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
	})
	if err != nil {
		t.Fatalf("get integrator with stale lead: %v", err)
	}
	if !staleLeadResult.HasWork || staleLeadResult.Task == nil || staleLeadResult.Task.TaskID != repairTaskID {
		t.Fatalf("expected integrator fallback to receive stale-lead repair task, got %+v", staleLeadResult)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      repairTaskID,
		AgentID:     integratorID,
		Summary:     "backstop stale strategic lead repair",
	}); err != nil {
		t.Fatalf("integrator fallback should claim stale-lead repair task: %v", err)
	}
}

func TestProjectClaimRepairTaskSupersededWhenConflictBranchNoLongerLive(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		registerBranch bool
	}{
		{name: "terminal_branch_present", registerBranch: true},
		{name: "branch_missing_from_registry", registerBranch: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := sqlite.NewTestStore(t)
			ctx := context.Background()
			workspaceID := "ws-project-claim-repair-stale-" + tc.name
			projectID := "project-claim-repair-stale-" + tc.name
			const (
				leadID       = "alpha"
				builderID    = "beta"
				repoID       = "repo-claim-repair-stale"
				branchID     = "branch-merged-or-missing"
				repairTaskID = "task-project-claim-repair-stale"
			)
			seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
			if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
				WorkspaceID: workspaceID,
				ProjectID:   projectID,
				Title:       "Stale repair routing",
				CreatedBy:   leadID,
			}); err != nil {
				t.Fatalf("create project: %v", err)
			}
			if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
				WorkspaceID:           workspaceID,
				ProjectID:             projectID,
				AgentID:               leadID,
				ActorID:               leadID,
				ActorType:             "agent",
				LeaseSeconds:          900,
				PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
				PromptContextSurface:  "project.lead.claim",
			}); err != nil {
				t.Fatalf("claim strategic lead: %v", err)
			}
			if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
				WorkspaceID:           workspaceID,
				ProjectID:             projectID,
				RepoID:                repoID,
				RemoteURL:             "file:///tmp/project-claim-repair-stale.git",
				RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
				DefaultBranch:         "main",
				RepoStatus:            sqlite.ProjectRepositoryStatusReady,
				IsCanonical:           true,
				ActorID:               leadID,
				ActorType:             "agent",
				PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
				PromptContextSurface:  "project.repository.upsert",
			}); err != nil {
				t.Fatalf("upsert project repository: %v", err)
			}
			if tc.registerBranch {
				if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
					WorkspaceID:           workspaceID,
					ProjectID:             projectID,
					RepoID:                repoID,
					BranchID:              branchID,
					AgentID:               builderID,
					BranchName:            "agent/beta/project-claim-repair-stale/scaffold",
					BranchKind:            "feature",
					HeadSHA:               "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					WriteScopeJSON:        `{"paths":["src/**"]}`,
					Status:                sqlite.ProjectBranchStatusMerged,
					ActorID:               builderID,
					ActorType:             "agent",
					PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", builderID),
					PromptContextSurface:  "project.branch.register",
				}); err != nil {
					t.Fatalf("register terminal branch: %v", err)
				}
			}
			graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
			if err := dag.ValidateGraph(graph); err != nil {
				t.Fatalf("validate graph: %v", err)
			}
			if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
				WorkspaceID: workspaceID,
				TaskID:      repairTaskID,
				OwnerUserID: "developer",
				Priority:    "critical",
				Title:       "Repair overlapping project claim",
				Description: strings.Join([]string{
					"A project implementation lane is blocked by an overlapping write scope.",
					"",
					"Conflict:",
					"- blocked_task_id: task-downstream",
					"- blocked_agent_id: gamma",
					"- repo_id: " + repoID,
					"- conflict_kind: live_branch",
					"- conflict_branch_id: " + branchID,
					"- conflict_branch_status: READY_FOR_REVIEW",
					"- conflict_write_scope_json: {\"paths\":[\"src/**\"]}",
				}, "\n"),
				TaskKind:     "COORDINATION",
				ProjectLane:  "strategy",
				ProjectID:    projectID,
				Tags:         []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
				TaskTemplate: "integration",
			}, graph); err != nil {
				t.Fatalf("create stale repair task: %v", err)
			}
			if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
				WorkspaceID: workspaceID,
				TaskID:      repairTaskID,
				LinkedBy:    "developer",
			}); err != nil {
				t.Fatalf("attach stale repair task: %v", err)
			}

			leadWork, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
				WorkspaceID: workspaceID,
				AgentID:     leadID,
			})
			if err != nil {
				t.Fatalf("get lead work: %v", err)
			}
			if leadWork.HasWork {
				t.Fatalf("expected stale repair task to be superseded, got %+v", leadWork)
			}
			triggered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
				WorkspaceID:     workspaceID,
				AgentID:         leadID,
				Trigger:         "runtime_switch_task",
				CandidateTaskID: repairTaskID,
				IncludePacket:   true,
			})
			if err != nil {
				t.Fatalf("get triggered lead work: %v", err)
			}
			if triggered.HasWork || triggered.Reason != "trigger_task_superseded" || triggered.Packet == nil || triggered.Packet.WorkType != "trigger_task_superseded" {
				t.Fatalf("expected triggered stale repair to return explicit superseded diagnostic, got %+v", triggered)
			}
		})
	}
}

func TestIntegratorSelectsIntegrationWorkAheadOfLeadOnlyRepair(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID     = "ws-integrator-integration-lane"
		projectID       = "project-integrator-integration-lane"
		leadID          = "alpha"
		integratorID    = "zeta"
		rootTaskID      = "task-root-strategy"
		repairTaskID    = "task-project-claim-repair-integrator"
		integrationTask = "task-integrate-accepted-branch"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, integratorID})
	if err := store.UpsertAgentProfile(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        integratorID,
		Specialization: "integration",
		Tags:           []string{"integrator"},
		Metadata: map[string]any{
			"default_work_mode": "integrator",
		},
	}); err != nil {
		t.Fatalf("upsert integrator profile: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Integrator lane routing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, task := range []sqlite.TaskCreateInput{
		{
			WorkspaceID:  workspaceID,
			TaskID:       rootTaskID,
			OwnerUserID:  "developer",
			Priority:     "high",
			Title:        "Project strategy root",
			Description:  "Strategic root task that should remain with the strategist.",
			TaskKind:     "COORDINATION",
			ProjectLane:  "strategy",
			ProjectID:    projectID,
			Tags:         []string{"strategy"},
			TaskTemplate: "generic",
		},
		{
			WorkspaceID:  workspaceID,
			TaskID:       repairTaskID,
			OwnerUserID:  "developer",
			Priority:     "critical",
			Title:        "Repair project claim scope conflict",
			Description:  "A project implementation lane is blocked by an overlapping write scope.\n\nConflict:\n- blocked_task_id: task-downstream\n- blocked_agent_id: beta\n- conflict_kind: unknown_overlap",
			TaskKind:     "COORDINATION",
			ProjectLane:  "strategy",
			ProjectID:    projectID,
			Tags:         []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
			TaskTemplate: "integration",
		},
		{
			WorkspaceID:  workspaceID,
			TaskID:       integrationTask,
			OwnerUserID:  "developer",
			Priority:     "high",
			Title:        "Integrate accepted project branch",
			Description:  "Merge accepted branch evidence into the canonical project target and publish integration status.",
			TaskKind:     "EXECUTION",
			ProjectLane:  "integration",
			ProjectID:    projectID,
			Tags:         []string{"integration", "project"},
			TaskTemplate: "generic",
		},
	} {
		if err := store.CreateTaskWithGraph(ctx, task, graph); err != nil {
			t.Fatalf("create task %s: %v", task.TaskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      task.TaskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", task.TaskID, err)
		}
	}

	next, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID: workspaceID,
		AgentID:     integratorID,
	})
	if err != nil {
		t.Fatalf("get integrator work: %v", err)
	}
	if !next.HasWork || next.Task == nil || next.Task.TaskID != integrationTask {
		t.Fatalf("expected integrator to select integration work ahead of lead-only repair/root strategy, got %+v", next)
	}
}

func TestProjectStrategicLeadLeaseAndRoleAdmission(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-lead-lease"
		projectID   = "project-lead-lease"
		leadA       = "lead-a"
		leadB       = "lead-b"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadA, leadB})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Lead Lease",
		CreatedBy:   leadA,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	lead, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadA,
		ActorID:               leadA,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadA),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim lead A: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadB,
		ActorID:               leadB,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadB),
		PromptContextSurface:  "project.lead.claim",
	}); err == nil {
		t.Fatal("expected second concurrent strategic lead claim to fail")
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadB,
		RoleType:              sqlite.ProjectRoleImplementer,
		ActorID:               leadA,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadA),
		PromptContextSurface:  "project.role.assign",
	}); err == nil {
		t.Fatal("expected implementation role without write scope to fail")
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE project_agent_roles SET lease_expires_at = '2026-04-27T00:00:00Z' WHERE role_id = ?`, lead.RoleID); err != nil {
		t.Fatalf("expire lead lease: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseSpec,
		ActorID:               leadA,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadA),
		PromptContextSurface:  "project.phase.transition",
	}); err == nil {
		t.Fatal("expected expired strategic lead to block phase transition")
	}
}

func TestTrustFirstTaskClaimTreatsImplementationRoleAsAdvisoryWhenProjectRolesExist(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-role-claim-fit"
		projectID   = "project-trust-first-role-claim-fit"
		leadID      = "alpha"
		builderID   = "beta"
		reviewerID  = "epsilon"
		taskID      = "task-trust-first-implementation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Trust-first Role Claim Fit",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	for _, role := range []struct {
		agentID string
		role    string
		scope   string
	}{
		{builderID, sqlite.ProjectRoleImplementer, `{"paths":["src/**","package.json"]}`},
		{reviewerID, sqlite.ProjectRoleReviewer, `{}`},
	} {
		if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               role.agentID,
			RoleType:              role.role,
			WriteScopeJSON:        role.scope,
			ActorID:               leadID,
			ActorType:             "agent",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
			PromptContextSurface:  "project.role.assign",
		}); err != nil {
			t.Fatalf("assign %s role to %s: %v", role.role, role.agentID, err)
		}
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          reviewerID,
		CoordinationMode: "trust_first",
		Summary:          "reviewer may claim implementation under trust-first soft governance",
	}); err != nil {
		t.Fatalf("expected reviewer trust-first claim to treat role fit as advisory: %v", err)
	}
}

func TestTrustFirstImplementationClaimFailsClosedBeforeLeadAndPhase(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-bootstrap-roleless"
		projectID   = "project-trust-first-bootstrap-roleless"
		agentID     = "beta"
		taskID      = "task-bootstrap-implementation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Trust-first Bootstrap Roleless",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		CoordinationMode: "trust_first",
		Summary:          "bootstrap may proceed before a lead or execution role matrix exists",
	}); !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "project gate") {
		t.Fatalf("expected trust-first implementation claim to fail closed before lead/phase, got %v", err)
	}
}

func TestTrustFirstImplementationClaimFailsClosedWhenRepoMissing(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-repo-missing"
		projectID   = "project-trust-first-repo-missing"
		leadID      = "alpha"
		agentID     = "beta"
		taskID      = "task-repo-missing-implementation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Trust-first Repo Missing",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	repoRequired := true
	repoStatus := sqlite.ProjectRepoStatusMissing
	repoURL := ""
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoRequired:          &repoRequired,
		RepoStatus:            &repoStatus,
		RepoURL:               &repoURL,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.profile.update",
	}); err != nil {
		t.Fatalf("mark repo missing: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          agentID,
		CoordinationMode: "trust_first",
		Summary:          "implementation must still wait for a materialized repository",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) ||
		!strings.Contains(err.Error(), "project gate") ||
		!strings.Contains(err.Error(), "repo_ready_or_not_required") {
		t.Fatalf("expected trust-first implementation claim to fail on repo gate, got %v", err)
	}
}

func TestTrustFirstImplementationClaimTreatsRoleAsAdvisoryAfterLeadExists(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-trust-first-lead-role-claim-fit"
		projectID   = "project-trust-first-lead-role-claim-fit"
		leadID      = "alpha"
		builderID   = "beta"
		otherID     = "gamma"
		taskID      = "task-lead-gated-implementation"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID, otherID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Trust-first Lead Role Claim Fit",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, leadID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		AgentID:          otherID,
		CoordinationMode: "trust_first",
		Summary:          "trust-first soft governance treats missing role as advisory",
	}); err != nil {
		t.Fatalf("expected trust-first implementation claim to treat missing role as advisory after lead exists: %v", err)
	}
}

func TestGetAgentWorkNextRequiresProjectGateFlagBlocksFrontendLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gated-frontend"
		projectID   = "project-gated-frontend"
		taskID      = "task-gated-frontend"
		agentID     = "frontend-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Gated Frontend",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "frontend", true)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get frontend work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_gate_closed" || result.ProjectLane != "frontend" || !result.RequiresProjectGate {
		t.Fatalf("expected requires_project_gate frontend task to be withheld, got %+v", result)
	}
}

func TestGetAgentWorkNextImplicitlyHardGatesImplementationLane(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-implicit-gate"
		projectID   = "project-implicit-gate"
		taskID      = "task-implicit-implementation-gate"
		agentID     = "builder-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Implicit Gate",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", false)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          agentID,
		IncludePacket:    true,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
	})
	if err != nil {
		t.Fatalf("get implementation work next: %v", err)
	}
	if result.HasWork || result.Reason != "project_gate_closed" || result.ProjectLane != "implementation" || !result.RequiresProjectGate {
		t.Fatalf("expected implicit hard gate for implementation lane, got %+v", result)
	}
	if result.Packet == nil || result.Packet.Gate == nil || result.Packet.RequiresProjectGate == false {
		t.Fatalf("expected hard gate packet with effective requires_project_gate=true, got %+v", result.Packet)
	}
}

func TestGetAgentWorkNextRoutesExpiredLeadToStrategyRecoveryBeforeGatePacket(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-lead-recovery"
		projectID   = "project-lead-recovery"
		rootTaskID  = "task-project-root-recovery"
		implTaskID  = "task-implementation-after-expired-lead"
		leadID      = "alpha"
		builderID   = "beta"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, builderID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Lead Recovery",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	requiresRootGate := false
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              rootTaskID,
		OwnerUserID:         "developer",
		Priority:            "critical",
		Title:               "Autonomous coordination root",
		Description:         "Strategic agent should maintain the project lease, coordinate builders, and create any needed subtasks.",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		ProjectID:           projectID,
		ProjectLane:         "strategy",
		RequiresProjectGate: requiresRootGate,
	}, graph); err != nil {
		t.Fatalf("create root task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach root task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      rootTaskID,
		AgentID:     leadID,
		Summary:     "claim strategy root before restart",
	}); err != nil {
		t.Fatalf("claim root task: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, implTaskID, "implementation", true)
	lead, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_agent_roles
   SET lease_expires_at = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND role_id = ?`,
		past, past, workspaceID, projectID, lead.RoleID); err != nil {
		t.Fatalf("expire strategic lead lease: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_profiles
   SET current_phase = ?, design_doc_id = ?, implementation_plan_doc_id = ?, repo_required = 0, repo_status = ?
 WHERE workspace_id = ? AND project_id = ?`,
		sqlite.ProjectPhaseImplementation, "project.design", "project.plan", sqlite.ProjectRepoStatusNotRequired, workspaceID, projectID); err != nil {
		t.Fatalf("mark project otherwise implementation-ready: %v", err)
	}
	recovered, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          leadID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  implTaskID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get former lead recovery work: %v", err)
	}
	if !recovered.HasWork || recovered.Reason != "project_strategic_lead_recovery" || recovered.Task == nil || recovered.Task.TaskID != rootTaskID {
		t.Fatalf("expected former lead to recover via strategy root before gate packet, got %+v", recovered)
	}

	blocked, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:      workspaceID,
		AgentID:          builderID,
		Trigger:          "runtime_switch_task",
		CandidateTaskID:  implTaskID,
		CoordinationMode: sqlite.CoordinationModeTrustFirst,
		IncludePacket:    true,
	})
	if err != nil {
		t.Fatalf("get non-lead blocked work: %v", err)
	}
	if blocked.HasWork || blocked.Reason != "project_gate_closed" || blocked.Task != nil || blocked.Packet == nil || blocked.Packet.Gate == nil {
		t.Fatalf("expected non-lead implementation agent to remain gate-blocked, got %+v", blocked)
	}
	if !strings.Contains(blocked.Packet.Gate.Summary, "strategic_lead_active") {
		t.Fatalf("expected strategic lead gate summary, got %+v", blocked.Packet.Gate)
	}
}

func TestGetAgentWorkNextStrategyLaneBypassesImplementationGateFlag(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gated-strategy-root"
		projectID   = "project-gated-strategy-root"
		taskID      = "task-gated-strategy-root"
		agentID     = "alpha"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Gated Strategy Root",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "critical",
		Title:               "Causal Board autonomous coordination run",
		Description:         "Strategist should create design, project roles, and implementation tasks.",
		ProjectID:           projectID,
		TaskKind:            "COORDINATION",
		TaskTemplate:        "integration",
		ProjectLane:         "strategy",
		RequiresProjectGate: true,
	}, graph); err != nil {
		t.Fatalf("create strategy task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach strategy task: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get strategy work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("expected strategy root to bypass implementation gate, got %+v", result)
	}
}

func TestReviewLaneBypassesImplementationClaimAdmission(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-review-admission"
		projectID   = "project-review-admission"
		taskID      = "task-review-admission"
		leadID      = "lead-agent"
		reviewerID  = "reviewer-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, reviewerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Review Admission",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleReviewer,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign reviewer role: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_profiles
   SET current_phase = ?, repo_required = 1, repo_status = ?
 WHERE workspace_id = ? AND project_id = ?`,
		sqlite.ProjectPhaseImplementation, sqlite.ProjectRepoStatusReady, workspaceID, projectID); err != nil {
		t.Fatalf("seed project implementation readiness: %v", err)
	}
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "review", true)

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       reviewerID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get review work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" || result.Task == nil || result.Task.TaskID != taskID {
		t.Fatalf("expected review lane to bypass implementation gate selection, got %+v", result)
	}

	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                taskID,
		AgentID:               reviewerID,
		Summary:               "claim read-only review task",
		PromptContextEnvelope: boundAgentTaskPromptContextEnvelope("agent.task.claim", workspaceID, taskID, reviewerID),
	}); err != nil {
		t.Fatalf("expected review lane claim without write scope to succeed: %v", err)
	}
}

func TestGetAgentWorkNextSkipsGateBlockedSessionForRunnableTask(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-gated-session-skip"
		projectID   = "project-gated-session-skip"
		blockedTask = "task-blocked-session"
		freeTask    = "task-free-planning"
		agentID     = "planner-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{agentID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Gated Session Skip",
		CreatedBy:   agentID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	openProjectImplementationPhaseForClaimTest(t, ctx, store, workspaceID, projectID, agentID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, blockedTask, "backend", true)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, freeTask, "planning", false)
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      blockedTask,
		AgentID:     agentID,
		Summary:     "claimed before project gates closed",
	}); err != nil {
		t.Fatalf("claim blocked task: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-gated-session",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      blockedTask,
		StartedAt:   "2026-04-28T00:00:00Z",
	}); err != nil {
		t.Fatalf("create blocked session: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_profiles
   SET current_phase = ?
 WHERE workspace_id = ? AND project_id = ?`,
		sqlite.ProjectPhaseSpec, workspaceID, projectID); err != nil {
		t.Fatalf("close implementation phase after session claim: %v", err)
	}

	result, err := store.GetAgentWorkNext(ctx, sqlite.AgentWorkNextFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		IncludePacket: true,
	})
	if err != nil {
		t.Fatalf("get work next: %v", err)
	}
	if !result.HasWork || result.Reason != "next_pending" || result.Task == nil || result.Task.TaskID != freeTask {
		t.Fatalf("expected runnable planning task instead of gated session dead-end, got %+v", result)
	}
}

func createProjectImplementationTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID string) {
	t.Helper()
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, taskID, "implementation", true)
}

func openProjectImplementationPhaseForClaimTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, leadID string) {
	t.Helper()
	designDocID := "doc." + projectID + ".design"
	implementationPlanDocID := "doc." + projectID + ".plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 leadID,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("seed project docs: %v", err)
	}
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim strategic lead: %v", err)
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "Implementation gates are satisfied for claim regression.",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition implementation phase: %v", err)
	}
}

func createProjectTaskWithLane(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, taskID, projectLane string, requiresProjectGate bool) {
	t.Helper()
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "implementation", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	writeScopeHints := activeProjectImplementerScopeHintsForTest(t, ctx, store, workspaceID, projectID)
	if len(writeScopeHints) == 0 {
		writeScopeHints = []string{"**"}
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		WorkspaceID:         workspaceID,
		TaskID:              taskID,
		OwnerUserID:         "developer",
		Priority:            "high",
		Title:               "Implement project slice",
		Description:         "This must wait for project design and implementation gates.",
		ProjectID:           projectID,
		TaskKind:            "EXECUTION",
		ProjectLane:         projectLane,
		RequiresProjectGate: requiresProjectGate,
		WriteScopeHints:     writeScopeHints,
	}, graph); err != nil {
		t.Fatalf("create project implementation task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach project implementation task: %v", err)
	}
}

func activeProjectImplementerScopeHintsForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID string) []string {
	t.Helper()
	rows, err := store.DB().QueryContext(ctx, `
SELECT COALESCE(write_scope_json, '')
  FROM project_agent_roles
 WHERE workspace_id = ?
   AND project_id = ?
   AND role_type = ?
   AND status = ?
 ORDER BY updated_at DESC`,
		workspaceID, projectID, sqlite.ProjectRoleImplementer, sqlite.ProjectRoleStatusActive)
	if err != nil {
		t.Fatalf("query active implementer scope hints: %v", err)
	}
	defer rows.Close()
	seen := map[string]struct{}{}
	var hints []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan active implementer scope hints: %v", err)
		}
		for _, path := range writeScopeHintPathsForTest(raw) {
			key := strings.ToLower(strings.TrimSpace(path))
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			hints = append(hints, path)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate active implementer scope hints: %v", err)
	}
	return hints
}

func writeScopeHintPathsForTest(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	var paths []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			if path := strings.TrimSpace(typed); path != "" {
				paths = append(paths, path)
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}

func findProjectGateForTest(t *testing.T, gates []sqlite.ProjectGateRecord, gateKey string) sqlite.ProjectGateRecord {
	t.Helper()
	for _, gate := range gates {
		if gate.GateKey == gateKey {
			return gate
		}
	}
	t.Fatalf("gate %q not found in %+v", gateKey, gates)
	return sqlite.ProjectGateRecord{}
}

func findCoordinationTaskForTest(t *testing.T, tasks []sqlite.WorkspaceTaskRecord, taskID string) sqlite.WorkspaceTaskRecord {
	t.Helper()
	for _, task := range tasks {
		if task.TaskID == taskID {
			return task
		}
	}
	t.Fatalf("task %q not found in %+v", taskID, tasks)
	return sqlite.WorkspaceTaskRecord{}
}
