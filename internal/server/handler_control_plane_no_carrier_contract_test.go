package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestQueueRebaseRollbackFailureCreateWithoutCarrierProofStripsCallerLineage(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-no-carrier-proof"
		eventID     = "evt-rollback-failure-create-no-carrier-proof"
		entityID    = "entity-rollback-failure-create-no-carrier-proof"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Rollback Failure Create No Carrier Proof",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:    workspaceID,
		FailureScope:   "rsp_anomaly_list",
		FailureTrigger: "verifier_late_fail_queue_list",
		FailureMessage: "queue-less rollback-failure create should strip caller lineage without carrier proof",
		EventID:        eventID,
		EntityID:       entityID,
		Family:         "verifier_pressure",
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-no-carrier-root",
			ProvenanceGroupID: "evt-stale-no-carrier-prov",
			ParentRefsJSON:    []string{"evt-stale-no-carrier-parent"},
		},
	})

	queue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     entityID,
	})
	if queue.Status != "OPEN" || queue.QueueType != "FOLLOW_UP" {
		t.Fatalf("unexpected rollback-failure queue artifact %+v", queue)
	}
	if queue.SourceKind != "rsp_anomaly_list" || queue.SourceID != entityID {
		t.Fatalf("unexpected queue source artifact %+v", queue)
	}

	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(queue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode queue-less recovery payload: %v", err)
	}
	if payload.EventID != eventID || payload.EntityID != entityID || payload.Family != "verifier_pressure" {
		t.Fatalf("unexpected queue-less recovery payload %+v", payload)
	}
	if gotLineage := rebaseRollbackFailurePayloadLineage(payload); rollbackFailureLineagePresent(gotLineage) {
		t.Fatalf("expected queue-less recovery payload lineage to be stripped without carrier proof, got %+v", gotLineage)
	}

	createdEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	})
	if createdEvent.RootCauseID != "" || createdEvent.ProvenanceGroupID != "" || createdEvent.ParentRefsJSON != "[]" {
		t.Fatalf("expected queue-less recovery runtime event lineage to stay empty, got %+v", createdEvent)
	}
}

func TestQueueRebaseRollbackFailureWithCurrentAnomalyContextStripsCallerLineageOnRepairOnlyFallback(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rollback-failure-create-repair-only-fallback"
		repairID    = "repair-rollback-failure-create-repair-only-fallback"
		eventID     = "evt-rollback-failure-create-repair-only-fallback"
		entityID    = "entity-rollback-failure-create-repair-only-fallback"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Rollback Failure Create Repair-Only Fallback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	h.queueRebaseRollbackFailureWithCurrentAnomalyContext(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "rsp_anomaly",
		FailureTrigger:  "verifier_late_fail_repair_only_fallback",
		FailureMessage:  "repair-only anomaly fallback should strip caller lineage without store-proven carrier",
		EventID:         eventID,
		SourceID:        entityID,
		EntityID:        entityID,
		Family:          "verifier_pressure",
		RepairTensionID: repairID,
		Lineage: rebaseRuntimeLineage{
			RootCauseID:       "evt-stale-repair-only-root",
			ProvenanceGroupID: "evt-stale-repair-only-prov",
			ParentRefsJSON:    []string{"evt-stale-repair-only-parent"},
		},
	})

	queue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "rsp_anomaly",
		RepairTensionID: repairID,
	})
	if queue.Status != "OPEN" || queue.QueueType != "FOLLOW_UP" {
		t.Fatalf("unexpected repair-only fallback rollback-failure queue %+v", queue)
	}
	if queue.SourceKind != "rsp_anomaly" || queue.SourceID != entityID {
		t.Fatalf("unexpected repair-only fallback source artifact %+v", queue)
	}

	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(queue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode repair-only fallback rollback-failure payload: %v", err)
	}
	if payload.RepairTensionID != repairID || payload.EventID != eventID || payload.EntityID != entityID || payload.Family != "verifier_pressure" {
		t.Fatalf("unexpected repair-only fallback rollback-failure payload %+v", payload)
	}
	if gotLineage := rebaseRollbackFailurePayloadLineage(payload); rollbackFailureLineagePresent(gotLineage) {
		t.Fatalf("expected repair-only fallback payload lineage to be stripped without store proof, got %+v", gotLineage)
	}

	createdEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	})
	if createdEvent.RootCauseID != "" || createdEvent.ProvenanceGroupID != "" || createdEvent.ParentRefsJSON != "[]" {
		t.Fatalf("expected repair-only fallback runtime event lineage to stay empty, got %+v", createdEvent)
	}
}
