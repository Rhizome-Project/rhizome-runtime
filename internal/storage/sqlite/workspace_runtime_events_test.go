package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocLifecycleAppendsRuntimeEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-doc-runtime",
		Title:       "Workspace Doc Runtime",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-doc-runtime")

	firstUpsertEvent, err := store.UpsertWorkspaceDocWithEvent(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-doc-runtime",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "Runtime journaling should follow doc lifecycle.",
		UpdatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	upserted := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-doc-runtime",
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    "current_context",
	})
	if upserted.ActorID != "developer" {
		t.Fatalf("expected upsert actor developer, got %+v", upserted)
	}
	var upsertPayload struct {
		WorkspaceID string `json:"workspace_id"`
		DocKey      string `json:"doc_key"`
		Title       string `json:"title"`
		UpdatedBy   string `json:"updated_by"`
	}
	decodeRuntimePayload(t, upserted.PayloadJSON, &upsertPayload)
	if upsertPayload.WorkspaceID != "ws-doc-runtime" || upsertPayload.DocKey != "current_context" || upsertPayload.Title != "Current Context" || upsertPayload.UpdatedBy != "developer" {
		t.Fatalf("unexpected upsert payload: %+v", upsertPayload)
	}
	if firstUpsertEvent != upserted {
		t.Fatalf("expected UpsertWorkspaceDocWithEvent to return persisted runtime row, returned=%+v persisted=%+v", firstUpsertEvent, upserted)
	}

	secondUpsertEvent, err := store.UpsertWorkspaceDocWithEvent(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-doc-runtime",
		DocKey:      "current_context",
		Title:       "Current Context v2",
		Content:     "Runtime journaling should keep exact repeated rows.",
		UpdatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("upsert workspace doc second revision: %v", err)
	}

	upsertEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-doc-runtime",
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    "current_context",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list repeated doc upsert runtime events: %v", err)
	}
	if len(upsertEvents) != 2 {
		t.Fatalf("expected two doc upsert runtime events, got %+v", upsertEvents)
	}
	secondUpserted := upsertEvents[0]
	if secondUpsertEvent != secondUpserted {
		t.Fatalf("expected second UpsertWorkspaceDocWithEvent to return persisted runtime row, returned=%+v persisted=%+v", secondUpsertEvent, secondUpserted)
	}
	if secondUpsertEvent.EventID == firstUpsertEvent.EventID || secondUpsertEvent.IngestSeq <= firstUpsertEvent.IngestSeq {
		t.Fatalf("expected second upsert to return a newer runtime row, first=%+v second=%+v", firstUpsertEvent, secondUpsertEvent)
	}
	decodeRuntimePayload(t, secondUpserted.PayloadJSON, &upsertPayload)
	if upsertPayload.Title != "Current Context v2" {
		t.Fatalf("expected second upsert payload to win, got %+v", upsertPayload)
	}

	archivedEvent, err := store.ArchiveWorkspaceDocWithEvent(ctx, "ws-doc-runtime", "current_context", "developer")
	if err != nil {
		t.Fatalf("archive workspace doc: %v", err)
	}

	archived := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-doc-runtime",
		EventType:   "workspace_doc.archived",
		EntityType:  "workspace_doc",
		EntityID:    "current_context",
	})
	var archivedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		DocKey      string `json:"doc_key"`
		ArchivedBy  string `json:"archived_by"`
	}
	decodeRuntimePayload(t, archived.PayloadJSON, &archivedPayload)
	if archivedPayload.WorkspaceID != "ws-doc-runtime" || archivedPayload.DocKey != "current_context" || archivedPayload.ArchivedBy != "developer" {
		t.Fatalf("unexpected archived payload: %+v", archivedPayload)
	}
	if archivedEvent.EventID != archived.EventID || archivedEvent.IngestSeq != archived.IngestSeq || archivedEvent.PayloadJSON != archived.PayloadJSON {
		t.Fatalf("expected ArchiveWorkspaceDocWithEvent to return persisted runtime row, returned=%+v persisted=%+v", archivedEvent, archived)
	}

	deletedEvent, err := store.DeleteWorkspaceDocWithEvent(ctx, "ws-doc-runtime", "current_context", "developer")
	if err != nil {
		t.Fatalf("delete workspace doc: %v", err)
	}

	deleted := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-doc-runtime",
		EventType:   "workspace_doc.deleted",
		EntityType:  "workspace_doc",
		EntityID:    "current_context",
	})
	var deletedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		DocKey      string `json:"doc_key"`
		DeletedBy   string `json:"deleted_by"`
	}
	decodeRuntimePayload(t, deleted.PayloadJSON, &deletedPayload)
	if deletedPayload.WorkspaceID != "ws-doc-runtime" || deletedPayload.DocKey != "current_context" || deletedPayload.DeletedBy != "developer" {
		t.Fatalf("unexpected deleted payload: %+v", deletedPayload)
	}
	if deletedEvent.EventID != deleted.EventID || deletedEvent.IngestSeq != deleted.IngestSeq || deletedEvent.PayloadJSON != deleted.PayloadJSON {
		t.Fatalf("expected DeleteWorkspaceDocWithEvent to return persisted runtime row, returned=%+v persisted=%+v", deletedEvent, deleted)
	}
}

func TestWorkspaceArtifactAndAgentUpdateAppendRuntimeEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-artifact-runtime",
		Title:       "Workspace Artifact Runtime",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-artifact-runtime")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-artifact-runtime",
		AgentID:     "agent-runtime",
		OwnerUserID: "developer",
		DisplayName: "Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, "ws-artifact-runtime", "task-artifact-runtime")

	recordedEvent, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:      "update-runtime",
		WorkspaceID:   "ws-artifact-runtime",
		AgentID:       "agent-runtime",
		UpdateType:    "progress",
		Summary:       "Artifact is ready for journaling",
		PayloadJSON:   `{"step":"artifact"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("record agent update: %v", err)
	}

	updateEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-artifact-runtime",
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		EntityID:    "update-runtime",
		AgentID:     "agent-runtime",
	})
	if updateEvent.ActorType != "agent" || updateEvent.ActorID != "agent-runtime" {
		t.Fatalf("unexpected update actor metadata: %+v", updateEvent)
	}
	var updatePayload struct {
		WorkspaceID   string `json:"workspace_id"`
		UpdateID      string `json:"update_id"`
		AgentID       string `json:"agent_id"`
		UpdateType    string `json:"update_type"`
		Summary       string `json:"summary"`
		RequiresHuman bool   `json:"requires_human"`
	}
	decodeRuntimePayload(t, updateEvent.PayloadJSON, &updatePayload)
	if updatePayload.WorkspaceID != "ws-artifact-runtime" || updatePayload.UpdateID != "update-runtime" || updatePayload.AgentID != "agent-runtime" || updatePayload.UpdateType != "progress" || updatePayload.Summary != "Artifact is ready for journaling" || !updatePayload.RequiresHuman {
		t.Fatalf("unexpected update payload: %+v", updatePayload)
	}
	if recordedEvent.EventID != updateEvent.EventID || recordedEvent.IngestSeq != updateEvent.IngestSeq || recordedEvent.PayloadJSON != updateEvent.PayloadJSON {
		t.Fatalf("expected RecordAgentUpdateWithEvent to return persisted runtime row, returned=%+v persisted=%+v", recordedEvent, updateEvent)
	}
	assertRuntimeEventAuthorityMetadata(t, recordedEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, updateEvent, authority)

	artifactRecord, artifactRuntimeEvent, err := store.RecordWorkspaceArtifactWithEvent(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-runtime",
		WorkspaceID:  "ws-artifact-runtime",
		TaskID:       "task-artifact-runtime",
		UpdateID:     "update-runtime",
		Title:        "Runtime Artifact",
		ArtifactRef:  "artifacts/runtime.md",
		Kind:         "document",
		ContentType:  "text/markdown",
		CreatedBy:    "agent-runtime",
		MetadataJSON: `{"origin":"test"}`,
	})
	if err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if artifactRecord.ArtifactID != "artifact-runtime" || artifactRuntimeEvent.EventID == "" {
		t.Fatalf("expected artifact record and exact runtime row, got record=%+v event=%+v", artifactRecord, artifactRuntimeEvent)
	}

	artifactEvent := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-artifact-runtime",
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    "artifact-runtime",
		TaskID:      "task-artifact-runtime",
	})
	if artifactEvent.ActorID != "agent-runtime" {
		t.Fatalf("expected artifact actor agent-runtime, got %+v", artifactEvent)
	}
	var artifactPayload struct {
		WorkspaceID string `json:"workspace_id"`
		ArtifactID  string `json:"artifact_id"`
		TaskID      string `json:"task_id"`
		UpdateID    string `json:"update_id"`
		Title       string `json:"title"`
		ArtifactRef string `json:"artifact_ref"`
		Kind        string `json:"kind"`
		ContentType string `json:"content_type"`
		CreatedBy   string `json:"created_by"`
	}
	decodeRuntimePayload(t, artifactEvent.PayloadJSON, &artifactPayload)
	if artifactPayload.WorkspaceID != "ws-artifact-runtime" || artifactPayload.ArtifactID != "artifact-runtime" || artifactPayload.TaskID != "task-artifact-runtime" || artifactPayload.UpdateID != "update-runtime" || artifactPayload.Title != "Runtime Artifact" || artifactPayload.ArtifactRef != "artifacts/runtime.md" || artifactPayload.Kind != "document" || artifactPayload.ContentType != "text/markdown" || artifactPayload.CreatedBy != "agent-runtime" {
		t.Fatalf("unexpected artifact payload: %+v", artifactPayload)
	}
	if artifactRuntimeEvent.EventID != artifactEvent.EventID || artifactRuntimeEvent.IngestSeq != artifactEvent.IngestSeq || artifactRuntimeEvent.PayloadJSON != artifactEvent.PayloadJSON {
		t.Fatalf("expected RecordWorkspaceArtifactWithEvent to return persisted runtime row, returned=%+v persisted=%+v", artifactRuntimeEvent, artifactEvent)
	}
	assertRuntimeEventAuthorityMetadata(t, artifactRuntimeEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, artifactEvent, authority)
}

func TestTaskClaimLifecycleAppendsRuntimeEvents(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-task-runtime",
		Title:       "Workspace Task Runtime",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-task-runtime")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-task-runtime",
		AgentID:     "agent-task-runtime",
		OwnerUserID: "developer",
		DisplayName: "Task Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, taskID := range []string{"task-claim-runtime", "task-complete-runtime", "task-block-runtime"} {
		createWorkspaceTask(t, ctx, store, "ws-task-runtime", taskID)
	}

	claimedEvent, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-claim-runtime",
		AgentID:     "agent-task-runtime",
		Summary:     "starting work",
	})
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}

	claimed := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-runtime",
		EventType:   "task.claimed",
		EntityType:  "task",
		EntityID:    "task-claim-runtime",
		TaskID:      "task-claim-runtime",
		AgentID:     "agent-task-runtime",
	})
	var claimedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		TaskID      string `json:"task_id"`
		AgentID     string `json:"agent_id"`
		Summary     string `json:"summary"`
	}
	decodeRuntimePayload(t, claimed.PayloadJSON, &claimedPayload)
	if claimedPayload.WorkspaceID != "ws-task-runtime" || claimedPayload.TaskID != "task-claim-runtime" || claimedPayload.AgentID != "agent-task-runtime" || claimedPayload.Summary != "starting work" {
		t.Fatalf("unexpected claimed payload: %+v", claimedPayload)
	}
	if claimedEvent.EventID != claimed.EventID || claimedEvent.IngestSeq != claimed.IngestSeq || claimedEvent.PayloadJSON != claimed.PayloadJSON {
		t.Fatalf("expected ClaimTaskWithEvent to return persisted runtime row, returned=%+v persisted=%+v", claimedEvent, claimed)
	}
	assertRuntimeEventAuthorityMetadata(t, claimedEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, claimed, authority)

	releasedEvent, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-claim-runtime",
		AgentID:     "agent-task-runtime",
		Reason:      "waiting on docs",
	})
	if err != nil {
		t.Fatalf("release task claim: %v", err)
	}

	released := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-runtime",
		EventType:   "task.released",
		EntityType:  "task",
		EntityID:    "task-claim-runtime",
		TaskID:      "task-claim-runtime",
		AgentID:     "agent-task-runtime",
	})
	var releasedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		TaskID      string `json:"task_id"`
		AgentID     string `json:"agent_id"`
		Reason      string `json:"reason"`
	}
	decodeRuntimePayload(t, released.PayloadJSON, &releasedPayload)
	if releasedPayload.WorkspaceID != "ws-task-runtime" || releasedPayload.TaskID != "task-claim-runtime" || releasedPayload.AgentID != "agent-task-runtime" || releasedPayload.Reason != "waiting on docs" {
		t.Fatalf("unexpected released payload: %+v", releasedPayload)
	}
	if releasedEvent.EventID != released.EventID || releasedEvent.IngestSeq != released.IngestSeq || releasedEvent.PayloadJSON != released.PayloadJSON {
		t.Fatalf("expected ReleaseTaskClaimWithEvent to return persisted runtime row, returned=%+v persisted=%+v", releasedEvent, released)
	}
	assertRuntimeEventAuthorityMetadata(t, releasedEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, released, authority)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-complete-runtime",
		AgentID:     "agent-task-runtime",
		Summary:     "finishing task",
	}); err != nil {
		t.Fatalf("claim completion task: %v", err)
	}
	completedEvent, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-complete-runtime",
		AgentID:     "agent-task-runtime",
		Summary:     "done and verified",
	})
	if err != nil {
		t.Fatalf("complete task: %v", err)
	}

	completed := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-runtime",
		EventType:   "task.completed",
		EntityType:  "task",
		EntityID:    "task-complete-runtime",
		TaskID:      "task-complete-runtime",
		AgentID:     "agent-task-runtime",
	})
	var completedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		TaskID      string `json:"task_id"`
		AgentID     string `json:"agent_id"`
		ClaimStatus string `json:"claim_status"`
		Summary     string `json:"summary"`
	}
	decodeRuntimePayload(t, completed.PayloadJSON, &completedPayload)
	if completedPayload.WorkspaceID != "ws-task-runtime" || completedPayload.TaskID != "task-complete-runtime" || completedPayload.AgentID != "agent-task-runtime" || completedPayload.ClaimStatus != "COMPLETED" || completedPayload.Summary != "done and verified" {
		t.Fatalf("unexpected completed payload: %+v", completedPayload)
	}
	if completedEvent.EventID != completed.EventID || completedEvent.IngestSeq != completed.IngestSeq || completedEvent.PayloadJSON != completed.PayloadJSON {
		t.Fatalf("expected CompleteTaskWithEvent to return persisted runtime row, returned=%+v persisted=%+v", completedEvent, completed)
	}
	assertRuntimeEventAuthorityMetadata(t, completedEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, completed, authority)

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-block-runtime",
		AgentID:     "agent-task-runtime",
		Summary:     "investigating blocker",
	}); err != nil {
		t.Fatalf("claim block task: %v", err)
	}
	blockedEvent, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: "ws-task-runtime",
		TaskID:      "task-block-runtime",
		AgentID:     "agent-task-runtime",
		Reason:      "need product decision",
	})
	if err != nil {
		t.Fatalf("block task: %v", err)
	}

	blocked := requireRuntimeEvent(t, store, ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-task-runtime",
		EventType:   "task.blocked",
		EntityType:  "task",
		EntityID:    "task-block-runtime",
		TaskID:      "task-block-runtime",
		AgentID:     "agent-task-runtime",
	})
	var blockedPayload struct {
		WorkspaceID string `json:"workspace_id"`
		TaskID      string `json:"task_id"`
		AgentID     string `json:"agent_id"`
		ClaimStatus string `json:"claim_status"`
		Reason      string `json:"reason"`
	}
	decodeRuntimePayload(t, blocked.PayloadJSON, &blockedPayload)
	if blockedPayload.WorkspaceID != "ws-task-runtime" || blockedPayload.TaskID != "task-block-runtime" || blockedPayload.AgentID != "agent-task-runtime" || blockedPayload.ClaimStatus != "BLOCKED" || blockedPayload.Reason != "need product decision" {
		t.Fatalf("unexpected blocked payload: %+v", blockedPayload)
	}
	if blockedEvent.EventID != blocked.EventID || blockedEvent.IngestSeq != blocked.IngestSeq || blockedEvent.PayloadJSON != blocked.PayloadJSON {
		t.Fatalf("expected BlockTaskWithEvent to return persisted runtime row, returned=%+v persisted=%+v", blockedEvent, blocked)
	}
	assertRuntimeEventAuthorityMetadata(t, blockedEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, blocked, authority)
}

func TestClaimTaskWithEventRejectsMissingWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-task-claim-missing-authority"
		taskID      = "task-d2a-task-claim-missing-authority"
		agentID     = "agent-d2a-task-claim-missing-authority"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Task Claim Missing Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D2A Task Claim Missing Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	_, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "should fail closed before claim side effects",
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

	assertNoTaskClaimRowForAuthorityReject(t, ctx, store, taskID)
	if got := mustTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusPending {
		t.Fatalf("expected pending task after authority reject, got %q", got)
	}
	if got := countTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_claimed", taskID); got != 0 {
		t.Fatalf("expected no task_claimed audit events after authority reject, got %d", got)
	}
	assertNoTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.claimed")
	if afterUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected missing-authority reject to return before authority journaling and keep workspace updated_at at %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestReleaseTaskClaimWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-task-release-stale-authority"
		taskID      = "task-d2a-task-release-stale-authority"
		agentID     = "agent-d2a-task-release-stale-authority"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Task Release Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D2A Task Release Stale Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority release",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-611")

	_, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "should fail closed under stale authority",
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

	afterClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after stale authority reject, got %q", got)
	}
	if got := countTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_released", taskID); got != 0 {
		t.Fatalf("expected no task_released audit events after authority reject, got %d", got)
	}
	assertNoTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.released")
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestCompleteTaskWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-task-complete-stale-authority"
		taskID      = "task-d2a-task-complete-stale-authority"
		agentID     = "agent-d2a-task-complete-stale-authority"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Task Complete Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D2A Task Complete Stale Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority completion",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-612")

	_, err := store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "should fail closed under stale authority",
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

	afterClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after stale authority reject, got %q", got)
	}
	if got := countTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_completed", taskID); got != 0 {
		t.Fatalf("expected no task_completed audit events after authority reject, got %d", got)
	}
	assertNoTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.completed")
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

func TestBlockTaskWithEventRejectsStaleWorkspaceAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-task-block-stale-authority"
		taskID      = "task-d2a-task-block-stale-authority"
		agentID     = "agent-d2a-task-block-stale-authority"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Task Block Stale Authority",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "D2A Task Block Stale Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createWorkspaceTask(t, ctx, store, workspaceID, taskID)
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "seed claimed task before stale authority block",
	}); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	beforeClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	beforeUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID)
	transferWorkspaceAuthorityToExternalPeer(t, ctx, store, workspaceID, current, "authnode-999-613")

	_, err := store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Reason:      "should fail closed under stale authority",
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

	afterClaim := mustTaskClaimRecordForAuthorityReject(t, ctx, store, workspaceID, taskID)
	if afterClaim != beforeClaim {
		t.Fatalf("expected stale authority reject not to mutate task claim, before=%+v after=%+v", beforeClaim, afterClaim)
	}
	if got := mustTaskStatusForAuthorityReject(t, ctx, store, taskID); got != model.TaskStatusRunning {
		t.Fatalf("expected running task after stale authority reject, got %q", got)
	}
	if got := countTaskClaimAuditEventsForAuthorityReject(t, ctx, store, "task_blocked", taskID); got != 0 {
		t.Fatalf("expected no task_blocked audit events after authority reject, got %d", got)
	}
	assertNoTaskRuntimeEventsForAuthorityReject(t, ctx, store, workspaceID, taskID, "task.blocked")
	assertTaskAuthorityRejectEvent(t, ctx, store, workspaceID, string(sqlite.AuthorityRejectStale))
	if afterUpdatedAt := mustTaskAuthorityWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt == beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to advance due to authority reject journaling, still %q", afterUpdatedAt)
	}
}

type taskClaimRecordForAuthorityReject struct {
	AgentID     string
	ClaimStatus string
	Summary     string
}

func mustTaskAuthorityWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func mustTaskStatusForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, taskID string) string {
	t.Helper()

	var status string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, taskID).Scan(&status); err != nil {
		t.Fatalf("load task status for %s: %v", taskID, err)
	}
	return status
}

func assertNoTaskClaimRowForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, taskID string) {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM task_claims WHERE task_id = ?`, taskID).Scan(&count); err != nil {
		t.Fatalf("count task_claim rows for %s: %v", taskID, err)
	}
	if count != 0 {
		t.Fatalf("expected no task_claim rows for %s, got %d", taskID, count)
	}
}

func mustTaskClaimRecordForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) taskClaimRecordForAuthorityReject {
	t.Helper()

	var record taskClaimRecordForAuthorityReject
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(agent_id, ''), COALESCE(claim_status, ''), COALESCE(summary, '') FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&record.AgentID, &record.ClaimStatus, &record.Summary); err != nil {
		t.Fatalf("load task claim for %s/%s: %v", workspaceID, taskID, err)
	}
	return record
}

func countTaskClaimAuditEventsForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, eventType, taskID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE event_type = ? AND entity_type = 'task_claim' AND entity_id = ?`, eventType, taskID).Scan(&count); err != nil {
		t.Fatalf("count %s audit events for %s: %v", eventType, taskID, err)
	}
	return count
}

func assertNoTaskRuntimeEventsForAuthorityReject(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID, eventType string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "task",
		EntityID:    taskID,
		TaskID:      taskID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list %s runtime events for %s: %v", eventType, taskID, err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no %s runtime events for %s, got %+v", eventType, taskID, events)
	}
}

func assertTaskAuthorityRejectEvent(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, wantRejectCode string) {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.AuthorityEventRejected,
		EntityType:  "workspace_authority",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejected events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected authority.rejected runtime event")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != wantRejectCode {
		t.Fatalf("expected authority reject code %q, got %+v", wantRejectCode, payload)
	}
}

func TestRuntimeEventsPreserveCanonicalEnvelopeAndReplayOrder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-envelope",
		Title:       "Runtime Envelope",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	const createdAt = "2026-03-22T15:00:00Z"
	canonicalPayload := `{"message":"canonical envelope","dedup_key":"D-bridge-1","root_cause_id":"RC-bridge-1","provenance_group_id":"PG-bridge-1"}`
	legacyPayload := `{"message":"legacy envelope"}`

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-a-legacy",
		WorkspaceID: "ws-runtime-envelope",
		EventType:   "bridge.signal",
		EntityType:  "bridge_signal",
		EntityID:    "bridge-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: legacyPayload,
		CreatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("record legacy runtime event: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-b-canonical",
		DedupKey:          "D-bridge-1",
		WorkspaceID:       "ws-runtime-envelope",
		EventType:         "bridge.signal",
		EntityType:        "bridge_signal",
		EntityID:          "bridge-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "RC-bridge-1",
		ProvenanceGroupID: "PG-bridge-1",
		ParentRefsJSON:    `["rtev-a-legacy"]`,
		PayloadJSON:       canonicalPayload,
		CreatedAt:         createdAt,
	}); err != nil {
		t.Fatalf("record canonical runtime event: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-envelope",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 2 || events[0].EventID != "rtev-b-canonical" || events[1].EventID != "rtev-a-legacy" {
		t.Fatalf("expected reverse-ordered canonical replay events, got %+v", events)
	}
	if events[0].DedupKey != "D-bridge-1" || events[0].RootCauseID != "RC-bridge-1" || events[0].ProvenanceGroupID != "PG-bridge-1" {
		t.Fatalf("expected canonical envelope fields on runtime event record, got %+v", events[0])
	}
	if events[0].ParentRefsJSON != `["rtev-a-legacy"]` {
		t.Fatalf("expected canonical parent refs json to round-trip, got %+v", events[0])
	}
	if events[1].DedupKey != "" || events[1].RootCauseID != "" || events[1].ProvenanceGroupID != "" || events[1].ParentRefsJSON != "[]" {
		t.Fatalf("expected legacy runtime event record to stay additive-compatible, got %+v", events[1])
	}

	var canonicalPayloadOut struct {
		Message           string `json:"message"`
		DedupKey          string `json:"dedup_key"`
		RootCauseID       string `json:"root_cause_id"`
		ProvenanceGroupID string `json:"provenance_group_id"`
	}
	decodeRuntimePayload(t, events[0].PayloadJSON, &canonicalPayloadOut)
	if canonicalPayloadOut.Message != "canonical envelope" || canonicalPayloadOut.DedupKey != "D-bridge-1" || canonicalPayloadOut.RootCauseID != "RC-bridge-1" || canonicalPayloadOut.ProvenanceGroupID != "PG-bridge-1" {
		t.Fatalf("unexpected canonical runtime payload: %+v", canonicalPayloadOut)
	}

	var legacyPayloadOut struct {
		Message           string `json:"message"`
		DedupKey          string `json:"dedup_key"`
		RootCauseID       string `json:"root_cause_id"`
		ProvenanceGroupID string `json:"provenance_group_id"`
	}
	decodeRuntimePayload(t, events[1].PayloadJSON, &legacyPayloadOut)
	if legacyPayloadOut.Message != "legacy envelope" || legacyPayloadOut.DedupKey != "" || legacyPayloadOut.RootCauseID != "" || legacyPayloadOut.ProvenanceGroupID != "" {
		t.Fatalf("expected legacy runtime payload to stay backward compatible, got %+v", legacyPayloadOut)
	}

	report1, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-envelope",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report1.Evaluation.Verdict != "pass" || len(report1.Evaluation.Findings) != 0 {
		t.Fatalf("expected canonical replay to remain clean, got %+v", report1.Evaluation)
	}
	if len(report1.Events) != 2 || report1.Events[0].EventID != "rtev-b-canonical" || report1.Events[1].EventID != "rtev-a-legacy" {
		t.Fatalf("expected replay to preserve canonical event order, got %+v", report1.Events)
	}
	if report1.Events[0].DedupKey != "D-bridge-1" || report1.Events[0].RootCauseID != "RC-bridge-1" || report1.Events[0].ProvenanceGroupID != "PG-bridge-1" || report1.Events[0].ParentRefsJSON != `["rtev-a-legacy"]` {
		t.Fatalf("expected replay to preserve canonical runtime event fields, got %+v", report1.Events[0])
	}

	report2, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-envelope",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal second pass: %v", err)
	}
	if report2.Evaluation.Verdict != report1.Evaluation.Verdict || len(report2.Events) != len(report1.Events) {
		t.Fatalf("expected idempotent replay verdict and event count, first=%+v second=%+v", report1, report2)
	}
	for idx := range report1.Events {
		if report1.Events[idx].EventID != report2.Events[idx].EventID || report1.Events[idx].PayloadJSON != report2.Events[idx].PayloadJSON {
			t.Fatalf("expected idempotent replay event ordering and payloads, first=%+v second=%+v", report1.Events, report2.Events)
		}
	}
}

func TestRuntimeEventsWithoutDedupKeyRemainIndependent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-legacy-independence",
		Title:       "Runtime Legacy Independence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-legacy-b",
		WorkspaceID: "ws-runtime-legacy-independence",
		EventType:   "legacy.signal",
		EntityType:  "legacy_event",
		EntityID:    "legacy-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"message":"legacy b"}`,
		CreatedAt:   "2026-03-22T19:00:00Z",
	}); err != nil {
		t.Fatalf("record legacy event b: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-legacy-a",
		WorkspaceID: "ws-runtime-legacy-independence",
		EventType:   "legacy.signal",
		EntityType:  "legacy_event",
		EntityID:    "legacy-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"message":"legacy a"}`,
		CreatedAt:   "2026-03-22T19:00:00Z",
	}); err != nil {
		t.Fatalf("record legacy event a: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-legacy-independence",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 2 || events[0].EventID != "rtev-legacy-a" || events[1].EventID != "rtev-legacy-b" {
		t.Fatalf("expected legacy events to remain independently stored and follow ingest order, got %+v", events)
	}
	if events[0].DedupKey != "" || events[1].DedupKey != "" {
		t.Fatalf("expected legacy events to remain dedup_key-free, got %+v", events)
	}
	if events[0].IngestSeq != 2 || events[1].IngestSeq != 1 {
		t.Fatalf("expected legacy events to expose independent ingest sequence values, got %+v", events)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-legacy-independence",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if len(report.Events) != 2 || report.Events[0].EventID != "rtev-legacy-a" || report.Events[1].EventID != "rtev-legacy-b" {
		t.Fatalf("expected replay legacy events to follow ingest order, got %+v", report.Events)
	}
	if report.Evaluation.Verdict != "pass" || len(report.Events) != 2 {
		t.Fatalf("expected legacy replay to remain pass-clean with two events, got %+v", report)
	}
	if report.Events[0].IngestSeq != 2 || report.Events[1].IngestSeq != 1 {
		t.Fatalf("expected legacy replay to expose stable ingest sequence values, got %+v", report.Events)
	}
}

func TestRuntimeReplaySuppressesEquivalentDedupKeyDuplicates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-dedup",
		Title:       "Runtime Replay Dedup",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:     "rtev-replay-dedup-a",
		WorkspaceID: "ws-runtime-replay-dedup",
		EventType:   "operator_queue.opened",
		EntityType:  "operator_queue",
		EntityID:    "queue-dedup-1",
		ActorType:   "system",
		ActorID:     "tests",
		PayloadJSON: `{"queue_key":"queue:dedup","queue_type":"FOLLOW_UP","status":"OPEN","title":"Dedup queue","summary":"Replay should apply once","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true,"dedup_key":"queue:dedup:event-1","root_cause_id":"root-dedup-1","provenance_group_id":"prov-dedup-1"}`,
		CreatedAt:   "2026-03-22T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("record replay dedup event a: %v", err)
	}
	second, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-replay-dedup-b",
		DedupKey:          "queue:dedup:event-1",
		WorkspaceID:       "ws-runtime-replay-dedup",
		EventType:         "operator_queue.opened",
		EntityType:        "operator_queue",
		EntityID:          "queue-dedup-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-dedup-1",
		ProvenanceGroupID: "prov-dedup-1",
		PayloadJSON:       `{"summary":"Replay should apply once","source_id":"tests","assigned_to":"developer","source_kind":"manual","queue_type":"FOLLOW_UP","queue_key":"queue:dedup","urgency":"HIGH","keep_session_active":true,"status":"OPEN","title":"Dedup queue"}`,
		CreatedAt:         "2026-03-22T16:01:00Z",
	})
	if err != nil {
		t.Fatalf("record replay dedup event b: %v", err)
	}
	if second.EventID != first.EventID {
		t.Fatalf("expected equivalent dedup-key runtime event to reuse existing row, first=%+v second=%+v", first, second)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-dedup",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected equivalent dedup-key duplicates to stay pass-clean, got %+v", report.Evaluation)
	}
	if report.Metrics.TotalEvents != 1 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 0 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("unexpected replay dedup metrics %+v", report.Metrics)
	}
	queue := requireReplayQueue(t, report, "queue:dedup")
	if queue.EventCount != 1 || queue.Status != "OPEN" {
		t.Fatalf("expected replay queue to apply equivalent duplicate only once, got %+v", queue)
	}
}

func TestRuntimeReplayFlagsConflictingDedupKeyReuse(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-replay-dedup-conflict",
		Title:       "Runtime Replay Dedup Conflict",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	payloads := []struct {
		eventType string
		payload   string
		createdAt string
	}{
		{
			eventType: "operator_queue.opened",
			payload:   `{"queue_key":"queue:conflict","queue_type":"FOLLOW_UP","status":"OPEN","title":"Conflict queue","summary":"Opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests"}`,
			createdAt: "2026-03-22T17:00:00Z",
		},
		{
			eventType: "operator_queue.resolved",
			payload:   `{"queue_key":"queue:conflict","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Conflict queue","summary":"Resolved later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","resolution":"done","resolved_by":"developer"}`,
			createdAt: "2026-03-22T17:01:00Z",
		},
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-replay-conflict-a",
		DedupKey:          "queue:conflict:event-1",
		WorkspaceID:       "ws-runtime-replay-dedup-conflict",
		EventType:         payloads[0].eventType,
		EntityType:        "operator_queue",
		EntityID:          "queue-conflict-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-conflict-1",
		ProvenanceGroupID: "prov-conflict-1",
		PayloadJSON:       payloads[0].payload,
		CreatedAt:         payloads[0].createdAt,
	}); err != nil {
		t.Fatalf("record replay conflict event a: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, sqlite.RuntimeEventInput{
		EventID:           "rtev-replay-conflict-b",
		DedupKey:          "queue:conflict:event-1",
		WorkspaceID:       "ws-runtime-replay-dedup-conflict",
		EventType:         payloads[1].eventType,
		EntityType:        "operator_queue",
		EntityID:          "queue-conflict-1",
		ActorType:         "system",
		ActorID:           "tests",
		RootCauseID:       "root-conflict-1",
		ProvenanceGroupID: "prov-conflict-1",
		PayloadJSON:       payloads[1].payload,
		CreatedAt:         payloads[1].createdAt,
	}); err == nil {
		t.Fatal("expected conflicting dedup-key runtime event to fail")
	} else if !strings.Contains(err.Error(), "dedup_key") {
		t.Fatalf("expected dedup_key conflict, got %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-replay-dedup-conflict",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected rejected conflicting dedup-key reuse to keep replay pass-clean, got %+v", report.Evaluation)
	}
	if report.Metrics.TotalEvents != 1 || report.Metrics.AppliedEvents != 1 || report.Metrics.SuppressedDuplicateEvents != 0 || report.Metrics.ConflictingDuplicateKeys != 0 {
		t.Fatalf("unexpected replay conflict metrics %+v", report.Metrics)
	}
	if len(report.Evaluation.Findings) != 0 {
		t.Fatalf("expected no replay findings after storage rejected the conflict, got %+v", report.Evaluation.Findings)
	}
	queue := requireReplayQueue(t, report, "queue:conflict")
	if queue.EventCount != 1 || queue.Status != "OPEN" {
		t.Fatalf("expected surviving canonical queue state to remain visible, got %+v", queue)
	}
}

func TestRuntimeEventsListPrefersIngestSequenceOverTimestampAndEventID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	requireRuntimeEventIngestSequenceSupport(t, ctx, store)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-ingest-list",
		Title:       "Runtime Ingest List",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, event := range []sqlite.RuntimeEventInput{
		{
			EventID:     "rtev-z-same-ts",
			WorkspaceID: "ws-runtime-ingest-list",
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-seq",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"first ingest at equal timestamp"}`,
			CreatedAt:   "2026-03-22T21:00:00Z",
		},
		{
			EventID:     "rtev-a-same-ts",
			WorkspaceID: "ws-runtime-ingest-list",
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-seq",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"second ingest at equal timestamp"}`,
			CreatedAt:   "2026-03-22T21:00:00Z",
		},
		{
			EventID:     "rtev-backfill-old",
			WorkspaceID: "ws-runtime-ingest-list",
			EventType:   "legacy.signal",
			EntityType:  "legacy_event",
			EntityID:    "legacy-seq",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"message":"backfilled legacy event ingested last"}`,
			CreatedAt:   "2026-03-20T21:00:00Z",
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record runtime event %s: %v", event.EventID, err)
		}
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-runtime-ingest-list",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected three runtime events, got %+v", events)
	}
	if events[0].EventID != "rtev-backfill-old" || events[1].EventID != "rtev-a-same-ts" || events[2].EventID != "rtev-z-same-ts" {
		t.Fatalf("expected list order to follow ingest sequence, got %+v", events)
	}
	if events[0].IngestSeq != 3 || events[1].IngestSeq != 2 || events[2].IngestSeq != 1 {
		t.Fatalf("expected monotonic ingest sequence values, got %+v", events)
	}
}

func TestRuntimeReplayUsesIngestSequenceForBackfilledCausalOrder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	requireRuntimeEventIngestSequenceSupport(t, ctx, store)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-runtime-ingest-replay",
		Title:       "Runtime Ingest Replay",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, event := range []sqlite.RuntimeEventInput{
		{
			EventID:     "rtev-queue-open",
			WorkspaceID: "ws-runtime-ingest-replay",
			EventType:   "operator_queue.opened",
			EntityType:  "operator_queue",
			EntityID:    "queue-ingest-1",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"queue_key":"queue:ingest-order","queue_type":"FOLLOW_UP","status":"OPEN","title":"Ingest queue","summary":"opened first","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":true}`,
			CreatedAt:   "2026-03-22T22:00:00Z",
		},
		{
			EventID:     "rtev-queue-resolved-backfill",
			WorkspaceID: "ws-runtime-ingest-replay",
			EventType:   "operator_queue.resolved",
			EntityType:  "operator_queue",
			EntityID:    "queue-ingest-1",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"queue_key":"queue:ingest-order","queue_type":"FOLLOW_UP","status":"RESOLVED","title":"Ingest queue","summary":"backfilled resolution ingested later","assigned_to":"developer","urgency":"HIGH","source_kind":"manual","source_id":"tests","keep_session_active":false,"resolution":"done","resolved_by":"developer"}`,
			CreatedAt:   "2026-03-20T22:00:00Z",
		},
	} {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record runtime event %s: %v", event.EventID, err)
		}
	}

	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: "ws-runtime-ingest-replay",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}
	if len(report.Events) != 2 {
		t.Fatalf("expected two replay events, got %+v", report.Events)
	}
	if report.Events[0].EventID != "rtev-queue-resolved-backfill" || report.Events[1].EventID != "rtev-queue-open" {
		t.Fatalf("expected replay event list to follow ingest sequence, got %+v", report.Events)
	}
	if report.Events[0].IngestSeq != 2 || report.Events[1].IngestSeq != 1 {
		t.Fatalf("expected replay report to expose ingest sequence values, got %+v", report.Events)
	}
	queue := requireReplayQueue(t, report, "queue:ingest-order")
	if queue.Status != "RESOLVED" || queue.EventCount != 2 {
		t.Fatalf("expected replay reducer to apply backfilled event by ingest order, got %+v", queue)
	}
}

func createWorkspaceTask(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, taskID string) {
	t.Helper()

	createSingleNodeTask(t, ctx, store, taskID, taskID+"-node")
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s to workspace %s: %v", taskID, workspaceID, err)
	}
}

func requireRuntimeEvent(t *testing.T, store *sqlite.Store, ctx context.Context, filter sqlite.RuntimeEventFilter) sqlite.RuntimeEventRecord {
	t.Helper()

	if filter.Limit <= 0 {
		filter.Limit = 10
	}
	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly one runtime event for %+v, got %+v", filter, events)
	}
	return events[0]
}

func decodeRuntimePayload(t *testing.T, raw string, dst any) {
	t.Helper()

	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		t.Fatalf("decode runtime payload %q: %v", raw, err)
	}
}

func requireRuntimeEventIngestSequenceSupport(t *testing.T, ctx context.Context, store *sqlite.Store) {
	t.Helper()

	rows, err := store.DB().QueryContext(ctx, `PRAGMA table_info(runtime_events)`)
	if err != nil {
		t.Fatalf("query runtime_events table info: %v", err)
	}
	defer rows.Close()

	var (
		cid       int
		name      string
		typ       string
		notNull   int
		defaultV  any
		primaryK  int
		hasColumn bool
	)
	for rows.Next() {
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultV, &primaryK); err != nil {
			t.Fatalf("scan runtime_events table info: %v", err)
		}
		if strings.EqualFold(strings.TrimSpace(name), "ingest_seq") {
			hasColumn = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate runtime_events table info: %v", err)
	}
	if !hasColumn {
		t.Fatalf("expected runtime_events.ingest_seq support to be landed")
	}
}
