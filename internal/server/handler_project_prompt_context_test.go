package server

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectMutationsRecordActorBoundPromptContextEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-evidence"
	const actorID = "operator-a"
	const projectID = "project-alpha"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectMutationWorkspace(t, ctx, store, workspaceID, actorID)
	authority, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Alpha",
		Description: "Project CRUD evidence",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.created",
		EntityType:  "project",
		EntityID:    projectID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.created"), createRuntime, "project.created")
	assertServerRuntimeEventAuthorityMetadata(t, createRuntime, authority)
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, createRuntime.PayloadJSON), "project.create", workspaceID, "human", actorID, projectID, actorID)

	updatedDescription := "Updated project CRUD evidence"
	if _, rpcErr := h.projectUpdate(ctx, mustJSONRaw(projectUpdateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Alpha Updated",
		Description: &updatedDescription,
		Status:      "ARCHIVED",
		ActorID:     actorID,
	})); rpcErr != nil {
		t.Fatalf("project.update rpc error: %+v", rpcErr)
	}
	updateRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.updated",
		EntityType:  "project",
		EntityID:    projectID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.updated"), updateRuntime, "project.updated")
	assertServerRuntimeEventAuthorityMetadata(t, updateRuntime, authority)
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, updateRuntime.PayloadJSON), "project.update", workspaceID, "human", actorID, projectID, actorID)

	if _, rpcErr := h.projectDelete(ctx, mustJSONRaw(projectDeleteParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
	})); rpcErr != nil {
		t.Fatalf("project.delete rpc error: %+v", rpcErr)
	}
	deleteRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.deleted",
		EntityType:  "project",
		EntityID:    projectID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.deleted"), deleteRuntime, "project.deleted")
	assertServerRuntimeEventAuthorityMetadata(t, deleteRuntime, authority)
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, deleteRuntime.PayloadJSON), "project.delete", workspaceID, "human", actorID, projectID, actorID)
}

func TestProjectMutationsFailClosedBeforeStorageOnActorMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-actor-mismatch"
	const projectID = "project-mismatch"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	seedProjectMutationWorkspace(t, ctx, store, workspaceID, "operator-a")

	result, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Mismatch",
		CreatedBy:   "operator-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched created_by actor to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no create result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match created_by" {
		t.Fatalf("unexpected mismatch error %+v", rpcErr)
	}
	if _, err := store.GetProject(ctx, workspaceID, projectID); err == nil {
		t.Fatal("mismatched actor create mutated project storage")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.created",
		EntityType:  "project",
		EntityID:    projectID,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("mismatched actor create recorded runtime events: %+v", events)
	}
}

func TestProjectAutonomousWorkspaceRPCsUseStorageAPI(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-autonomous-rpc"
	const actorID = "operator-a"
	const projectID = "project-wave-2"
	const leadAgentID = "agent-project-wave-2-lead"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectMutationWorkspace(t, ctx, store, workspaceID, actorID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     leadAgentID,
		OwnerUserID: actorID,
		DisplayName: "Project Wave 2 Lead",
	}); err != nil {
		t.Fatalf("register lead agent: %v", err)
	}

	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Wave 2",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}

	goal := "Coordinate project-centric autonomous workspace implementation"
	repoRequired := true
	repoStatus := sqlite.ProjectRepoStatusReady
	repoURL := "https://example.test/rhizome.git"
	branch := "main"
	designDocID := "doc-design-wave-2"
	implementationPlanDocID := "doc-plan-wave-2"
	profileUpdate, rpcErr := h.projectProfileUpdate(ctx, mustJSONRaw(projectProfileUpdateParams{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		ActorID:                 actorID,
		Goal:                    &goal,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		RepoRequired:            &repoRequired,
		RepoStatus:              &repoStatus,
		RepoURL:                 &repoURL,
		RepoDefaultBranch:       &branch,
	}))
	if rpcErr != nil {
		t.Fatalf("project.profile.update rpc error: %+v", rpcErr)
	}
	updatedProfile, ok := profileUpdate.(map[string]any)["profile"].(sqlite.ProjectProfileRecord)
	if !ok {
		t.Fatalf("project.profile.update returned unexpected payload: %+v", profileUpdate)
	}
	if updatedProfile.Goal != goal || !updatedProfile.RepoRequired || updatedProfile.RepoStatus != sqlite.ProjectRepoStatusReady {
		t.Fatalf("project.profile.update returned wrong profile: %+v", updatedProfile)
	}
	profileRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.profile.updated",
		EntityType:  "project",
		EntityID:    projectID,
	})
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, profileRuntime.PayloadJSON), "project.profile.update", workspaceID, "human", actorID, projectID, actorID)

	profileGet, rpcErr := h.projectProfileGet(ctx, mustJSONRaw(projectProfileGetParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.profile.get rpc error: %+v", rpcErr)
	}
	readProfile, ok := profileGet.(map[string]any)["profile"].(sqlite.ProjectProfileRecord)
	if !ok || readProfile.Goal != goal {
		t.Fatalf("project.profile.get returned unexpected profile: %+v", profileGet)
	}

	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadAgentID,
		ActorID:               actorID,
		ActorType:             "human",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "human", actorID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim project strategic lead: %v", err)
	}

	transition, rpcErr := h.projectPhaseTransition(ctx, mustJSONRaw(projectPhaseTransitionParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		ToPhase:     sqlite.ProjectPhaseImplementation,
		Reason:      "storage API is ready",
	}))
	if rpcErr != nil {
		t.Fatalf("project.phase.transition rpc error: %+v", rpcErr)
	}
	history, ok := transition.(map[string]any)["history"].(sqlite.ProjectPhaseHistoryRecord)
	if !ok || history.FromPhase != sqlite.ProjectPhaseIntake || history.ToPhase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("project.phase.transition returned unexpected history: %+v", transition)
	}
	phaseRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.phase.transitioned",
		EntityType:  "project",
		EntityID:    projectID,
	})
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, phaseRuntime.PayloadJSON), "project.phase.transition", workspaceID, "human", actorID, projectID, actorID)

	gateStatus, rpcErr := h.projectGatesStatus(ctx, mustJSONRaw(projectGatesStatusParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.gates.status rpc error: %+v", rpcErr)
	}
	status, ok := gateStatus.(map[string]any)["gate_status"].(sqlite.ProjectGateStatusRecord)
	if !ok || status.CurrentPhase != sqlite.ProjectPhaseImplementation || !status.ImplementationReady {
		t.Fatalf("project.gates.status returned unexpected status: %+v", gateStatus)
	}

	coordination, rpcErr := h.projectCoordinationGet(ctx, mustJSONRaw(projectCoordinationGetParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.coordination.get rpc error: %+v", rpcErr)
	}
	coordinationRecord, ok := coordination.(map[string]any)["coordination"].(sqlite.ProjectCoordinationRecord)
	if !ok || coordinationRecord.Project.ProjectID != projectID || coordinationRecord.Profile.CurrentPhase != sqlite.ProjectPhaseImplementation {
		t.Fatalf("project.coordination.get returned unexpected coordination: %+v", coordination)
	}
}

func seedProjectMutationWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure local workspace authority for %s: %v", workspaceID, err)
	}
}

func assertProjectRuntimePromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, projectID, actorID string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected project prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	for key, want := range map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_project_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"project_id":                         projectID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}
