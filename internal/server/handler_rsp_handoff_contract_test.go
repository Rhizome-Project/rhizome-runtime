package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProcessRSPEventDoesNotMaterializeGovernedTensionFromBareAnomalyAlert(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-rsp-handoff-contract"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Handoff Contract",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	h.processRSPEvent(ctx, EventMessage{
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		PayloadJSON: `{"family":"thrashing","entity_id":"entity-1","cluster_id":"cluster-1"}`,
	})

	tensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	if len(tensions) != 0 {
		t.Fatalf("expected bare anomaly alert to stay advisory-only, got tensions=%+v", tensions)
	}
}

func TestProcessRSPEventIgnoresInternalEphemeralMotifSignals(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()
	workspaceID := "ws-rsp-motif-ephemeral-contract"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Motif Ephemeral Contract",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	for _, msg := range []EventMessage{
		{
			Type:        motifThrashEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     "agent-1",
			PayloadJSON: `{"tension_id":"entity-1","reason":"N>=3 verifier fails in 10m"}`,
		},
		{
			Type:        motifBounceEphemeralEventType,
			WorkspaceID: workspaceID,
			AgentID:     "agent-2",
			PayloadJSON: `{"artifact_ref":"artifact://1","conflicting_agent":"agent-1"}`,
		},
	} {
		h.processRSPEvent(ctx, msg)
	}

	tensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	if len(tensions) != 0 {
		t.Fatalf("expected internal motif signals to stay off governed tension path, got tensions=%+v", tensions)
	}
}

func TestStartRSPListenerMirrorsFirehoseAnomalyAlertIntoGovernedTension(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.StartRSPListener(ctx)

	workspaceID := "ws-rsp-live-handoff"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Live Handoff",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tester",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed handoff",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	events := []sqlite.RuntimeEventInput{
		{
			EventID:     "evt-rsp-handoff-1",
			WorkspaceID: workspaceID,
			EventType:   "artifact.patch",
			EntityType:  "artifact_ref",
			EntityID:    "artifact://thrash-loop",
			ActorType:   "agent",
			ActorID:     "agent-a",
			AgentID:     "agent-a",
			PayloadJSON: `{"summary":"patch 1"}`,
			CreatedAt:   "2026-03-27T10:00:00Z",
		},
		{
			EventID:     "evt-rsp-handoff-2",
			WorkspaceID: workspaceID,
			EventType:   "verifier.fail",
			EntityType:  "artifact_ref",
			EntityID:    "artifact://thrash-loop",
			ActorType:   "agent",
			ActorID:     "agent-a",
			AgentID:     "agent-a",
			PayloadJSON: `{"summary":"verifier fail"}`,
			CreatedAt:   "2026-03-27T10:00:01Z",
		},
		{
			EventID:     "evt-rsp-handoff-3",
			WorkspaceID: workspaceID,
			EventType:   "artifact.patch",
			EntityType:  "artifact_ref",
			EntityID:    "artifact://thrash-loop",
			ActorType:   "agent",
			ActorID:     "agent-a",
			AgentID:     "agent-a",
			PayloadJSON: `{"summary":"patch 2"}`,
			CreatedAt:   "2026-03-27T10:00:02Z",
		},
	}
	for _, event := range events {
		if _, err := store.RecordRuntimeEvent(ctx, event); err != nil {
			t.Fatalf("record runtime event %s: %v", event.EventID, err)
		}
	}

	var live EventMessage
	deadline := time.After(3 * time.Second)
	for live.Type == "" {
		select {
		case msg := <-ch:
			if msg.Type == "ANOMALY_ALERT" {
				live = msg
			}
		case <-deadline:
			t.Fatal("timed out waiting for live mirrored ANOMALY_ALERT")
		}
	}
	if live.EventID == "" || live.IngestSeq == 0 || live.EntityID != "artifact://thrash-loop" {
		t.Fatalf("expected canonical live mirrored anomaly event, got %+v", live)
	}

	var detail sqlite.TensionDetail
	found := false
	waitUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(waitUntil) {
		tensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
			WorkspaceID: workspaceID,
			Limit:       20,
		})
		if err != nil {
			t.Fatalf("list tensions: %v", err)
		}
		for _, tension := range tensions {
			if tension.TensionType == "dissent_followup" && tension.AnchorRef == "artifact://thrash-loop" {
				detail, err = store.GetTension(ctx, workspaceID, tension.TensionID)
				if err != nil {
					t.Fatalf("get governed tension: %v", err)
				}
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !found {
		t.Fatal("expected firehose anomaly alert to materialize governed tension via live handoff")
	}
	if detail.Tension.ProtoClusterID != "artifact://thrash-loop" || detail.Tension.LastSeenEventID != live.EventID {
		t.Fatalf("expected entity-scoped governed tension fallback, got %+v", detail.Tension)
	}
	if len(detail.Evidence) == 0 || detail.Evidence[0].EventID != live.EventID {
		t.Fatalf("expected governed tension evidence to point at live mirrored anomaly event, got %+v", detail.Evidence)
	}
}

func TestProcessRSPEventVerifierPressureRollsBackLinkedRebaseFollowupForRetry(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-late-fail"
		taskID      = "task-rsp-rebase-late-fail"
		agentID     = "agent-rsp-rebase-late-fail"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-late-fail"
		repairID    = "tens-repair-rsp-late-fail"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail rollback",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-late-fail",
		"fork_tension_id":     "tens-fork-rsp-late-fail",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for verifier pressure rollback",
		Details:           "Coalition ID: coal-rsp-late-fail\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before verifier pressure arrives.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-late-fail",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-late-fail","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "system:rsp" {
		t.Fatalf("action resolved_by = %q, want system:rsp", action.ResolvedBy)
	}
	if !strings.Contains(action.ResolutionComment, "RSP late verifier fail rollback") {
		t.Fatalf("action resolution comment did not surface anomaly rollback context: %q", action.ResolutionComment)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	if updatedSourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN", updatedSourceQueue.Status)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.ActionID != "" {
		t.Fatalf("expected source queue active action link to clear after anomaly rollback, got %q", payload.ActionID)
	}
	if payload.LastFailedActionID != actionID {
		t.Fatalf("last_failed_action_id = %q, want %q", payload.LastFailedActionID, actionID)
	}
	if payload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("rollback_reason = %q, want verifier_late_fail", payload.RollbackReason)
	}
	if payload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("payload workflow_state = %q, want %q", payload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("payload workflow_step = %q, want %q", payload.RebaseWorkflowStep, rebaseWorkflowStepAwaitRestart)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after anomaly rollback: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while retry remains open", claimStatus)
	}

	resolvedEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       1,
	})
	resolvedPayload := resolvedEvent.PayloadJSON
	if !strings.Contains(resolvedPayload, `"rollback_reason":"verifier_late_fail"`) {
		t.Fatalf("action.resolved payload did not surface verifier_late_fail: %s", resolvedPayload)
	}

	tensions, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	foundFailure := false
	for _, tension := range tensions {
		if tension.TensionType == "failure" && tension.AnchorRef == repairID {
			foundFailure = true
			break
		}
	}
	if !foundFailure {
		t.Fatalf("expected verifier_pressure anomaly to materialize failure tension for %s, got %+v", repairID, tensions)
	}
}

func TestProcessRSPEventVerifierPressureSupportsRetryPromotionAndCompletion(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-late-fail-retry-complete"
		taskID      = "task-rsp-rebase-late-fail-retry-complete"
		agentID     = "agent-rsp-rebase-late-fail-retry-complete"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-late-fail-retry-complete"
		repairID    = "tens-repair-rsp-late-fail-retry-complete"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail retry completion",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-late-fail-retry-complete",
		"fork_tension_id":     "tens-fork-rsp-late-fail-retry-complete",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for verifier pressure retry completion",
		Details:           "Coalition ID: coal-rsp-late-fail-retry-complete\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	failedActionID, ok := createResp["action_id"].(string)
	if !ok || failedActionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  failedActionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before verifier pressure retry path.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	const anomalyEventID = "evt-rsp-rebase-late-fail-retry-complete"
	seenRollbackQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	})
	seenFailedResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    failedActionID,
		Limit:       50,
	})
	h.processRSPEvent(ctx, EventMessage{
		EventID:     anomalyEventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-late-fail-retry-complete","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	rollbackQueueEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	}, seenRollbackQueueEvents)
	failedResolveEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    failedActionID,
		Limit:       50,
	}, seenFailedResolveEvents)
	if rollbackQueueEvent.RootCauseID != anomalyEventID || rollbackQueueEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("rollback source queue lineage = (%q,%q), want anomaly event %q", rollbackQueueEvent.RootCauseID, rollbackQueueEvent.ProvenanceGroupID, anomalyEventID)
	}
	if failedResolveEvent.RootCauseID != anomalyEventID || failedResolveEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("failed action resolve lineage = (%q,%q), want anomaly event %q", failedResolveEvent.RootCauseID, failedResolveEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, failedResolveEvent, rollbackQueueEvent.EventID)

	failedAction, err := store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", failedActionID, err)
	}
	if failedAction.ResolvedBy != "system:rsp" {
		t.Fatalf("failed action resolved_by = %q, want system:rsp", failedAction.ResolvedBy)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get reopened source queue: %v", err)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload: %v", err)
	}
	if reopenedPayload.ActionID != "" || reopenedPayload.ActionQueueKey != "" || reopenedPayload.ActionStatus != "" {
		t.Fatalf("reopened source queue should clear active action linkage, got %+v", reopenedPayload)
	}
	if reopenedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("last_failed_action_id = %q, want %q", reopenedPayload.LastFailedActionID, failedActionID)
	}
	if reopenedPayload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("rollback_reason = %q, want verifier_late_fail", reopenedPayload.RollbackReason)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim status after anomaly rollback: %v", err)
	}
	if claimStatus != "BLOCKED" {
		t.Fatalf("task claim status = %q, want BLOCKED while retry remains open", claimStatus)
	}

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueue.QueueID,
	}))
	if err == nil {
		t.Fatalf("expected happy anomaly rollback path to avoid rollback-failure recovery queue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found for rollback-failure queue lookup, got %v", err)
	}

	seenRetryCreateEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		Limit:       50,
	})
	seenRetrySourceUpdates := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	})
	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}
	if got, _ := retryCreateResp["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("retry actionCreate source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}
	retrySourceUpdateEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	}, seenRetrySourceUpdates)
	retryCreatedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    retryActionID,
		Limit:       50,
	}, seenRetryCreateEvents)
	if retrySourceUpdateEvent.RootCauseID != anomalyEventID || retrySourceUpdateEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("retry source queue lineage = (%q,%q), want anomaly event %q", retrySourceUpdateEvent.RootCauseID, retrySourceUpdateEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, retrySourceUpdateEvent, rollbackQueueEvent.EventID, failedResolveEvent.EventID)
	if retryCreatedEvent.RootCauseID != anomalyEventID || retryCreatedEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("retry action created lineage = (%q,%q), want anomaly event %q", retryCreatedEvent.RootCauseID, retryCreatedEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, retryCreatedEvent, retrySourceUpdateEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	retryLinkedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get retry-linked source queue: %v", err)
	}
	retryLinkedPayload, err := actionCreateDecodeQueuePayload(retryLinkedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode retry-linked source queue payload: %v", err)
	}
	if retryLinkedPayload.ActionID != retryActionID {
		t.Fatalf("active retry action_id = %q, want %q", retryLinkedPayload.ActionID, retryActionID)
	}
	if retryLinkedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("retry payload should preserve failed attempt lineage, got %+v", retryLinkedPayload)
	}
	if retryLinkedPayload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("retry payload rollback_reason = %q, want verifier_late_fail", retryLinkedPayload.RollbackReason)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "Retry after verifier-pressure anomaly rollback.",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	seenRetryStartQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	})
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}
	retryStartQueueEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	}, seenRetryStartQueueEvents)
	if retryStartQueueEvent.RootCauseID != anomalyEventID || retryStartQueueEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("retry start source queue lineage = (%q,%q), want anomaly event %q", retryStartQueueEvent.RootCauseID, retryStartQueueEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, retryStartQueueEvent, retrySourceUpdateEvent.EventID, failedResolveEvent.EventID)
	retryStartedEvent := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    retryActionID,
		Limit:       1,
	})
	if retryStartedEvent.RootCauseID != anomalyEventID || retryStartedEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("retry action started lineage = (%q,%q), want anomaly event %q", retryStartedEvent.RootCauseID, retryStartedEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, retryStartedEvent, retryStartQueueEvent.EventID)

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	seenRetryResolveEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    retryActionID,
		Limit:       50,
	})
	seenSourceResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	})
	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Retry landed cleanly after verifier-pressure anomaly.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	retryResolveAny, rpcErr := h.actionResolve(ctx, retryResolveRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}
	retryResolveResp, ok := retryResolveAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionResolve response type %T", retryResolveAny)
	}
	if got, _ := retryResolveResp["status"].(string); got != humanActionStatusCompleted {
		t.Fatalf("retry actionResolve status = %q, want %q", got, humanActionStatusCompleted)
	}
	if got, _ := retryResolveResp["source_queue_id"].(string); got != sourceQueue.QueueID {
		t.Fatalf("retry actionResolve source_queue_id = %q, want %q", got, sourceQueue.QueueID)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
	sourceResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    sourceQueue.QueueID,
		Limit:       50,
	}, seenSourceResolvedEvents)
	retryResolvedEvent := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    retryActionID,
		Limit:       50,
	}, seenRetryResolveEvents)
	if sourceResolvedEvent.RootCauseID != anomalyEventID || sourceResolvedEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("completed source queue lineage = (%q,%q), want anomaly event %q", sourceResolvedEvent.RootCauseID, sourceResolvedEvent.ProvenanceGroupID, anomalyEventID)
	}
	if retryResolvedEvent.RootCauseID != anomalyEventID || retryResolvedEvent.ProvenanceGroupID != anomalyEventID {
		t.Fatalf("retry action resolved lineage = (%q,%q), want anomaly event %q", retryResolvedEvent.RootCauseID, retryResolvedEvent.ProvenanceGroupID, anomalyEventID)
	}
	assertRuntimeEventParentRefsContain(t, retryResolvedEvent, sourceResolvedEvent.EventID)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get completed source queue: %v", err)
	}
	if completedSourceQueue.Status != "RESOLVED" {
		t.Fatalf("completed source queue status = %q, want RESOLVED", completedSourceQueue.Status)
	}
	if completedSourceQueue.Resolution != "linked_action_completed:"+retryActionID {
		t.Fatalf("completed source queue resolution = %q, want linked_action_completed:%s", completedSourceQueue.Resolution, retryActionID)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID {
		t.Fatalf("completed payload action_id = %q, want %q", completedPayload.ActionID, retryActionID)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve failed attempt lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("completed payload rollback_reason = %q, want verifier_late_fail", completedPayload.RollbackReason)
	}

	failedAction, err = store.GetHumanAction(ctx, failedActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s) after retry completion: %v", failedActionID, err)
	}
	if failedAction.Status != humanActionStatusFailed {
		t.Fatalf("failed action status after retry completion = %q, want %q", failedAction.Status, humanActionStatusFailed)
	}

	retryActionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", retryActionID)
	if retryActionQueue.Status != "RESOLVED" {
		t.Fatalf("retry action queue status = %q, want RESOLVED", retryActionQueue.Status)
	}

	if err := store.DB().QueryRowContext(ctx, `SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&claimStatus); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected task claim blocker snapshot to be cleared after successful anomaly retry, got status=%q err=%v", claimStatus, err)
	}
}

func TestProcessRSPEventVerifierPressureSupportsEscalatedRetryPromotionToNewHolder(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-late-fail-retry-handoff"
		taskID      = "task-rsp-rebase-late-fail-retry-handoff"
		agentID     = "agent-rsp-rebase-late-fail-retry-handoff"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-late-fail-retry-handoff"
		repairID    = "tens-repair-rsp-late-fail-retry-handoff"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail retry handoff",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-late-fail-retry-handoff",
		"fork_tension_id":     "tens-fork-rsp-late-fail-retry-handoff",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for verifier pressure retry handoff",
		Details:           "Coalition ID: coal-rsp-late-fail-retry-handoff\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	failedActionID, ok := createResp["action_id"].(string)
	if !ok || failedActionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  failedActionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before verifier pressure handoff retry path.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-late-fail-retry-handoff",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-late-fail-retry-handoff","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)

	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get source queue before handoff escalate: %v", err)
	}
	escalateRaw, err := json.Marshal(workspaceOpsEscalateParams{
		WorkspaceID:      workspaceID,
		QueueID:          sourceQueue.QueueID,
		EscalatedBy:      "lead-b",
		Reason:           "route anomaly retry to a different reviewer",
		AssignedTo:       "reviewer-b",
		Urgency:          "CRITICAL",
		DueAt:            "2099-08-01T00:00:00Z",
		CurrentRevision:  currentSourceQueue.Revision,
		CurrentUpdatedAt: currentSourceQueue.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("marshal workspaceOpsEscalate params: %v", err)
	}
	if _, rpcErr := h.workspaceOpsEscalate(testAuthContext(workspaceID, "system", "tests"), escalateRaw); rpcErr != nil {
		t.Fatalf("workspaceOpsEscalate rpc error: %+v", rpcErr)
	}

	reopenedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get reopened source queue after handoff: %v", err)
	}
	if reopenedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("reopened source queue assigned_to = %q, want reviewer-b", reopenedSourceQueue.AssignedTo)
	}
	reopenedPayload, err := actionCreateDecodeQueuePayload(reopenedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode reopened source queue payload after handoff: %v", err)
	}
	if reopenedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("reopened payload action_assigned_to = %q, want reviewer-b", reopenedPayload.ActionAssignedTo)
	}
	if reopenedPayload.LastFailedActionID != failedActionID || reopenedPayload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("reopened payload should preserve anomaly failed lineage through handoff, got %+v", reopenedPayload)
	}

	retryCreateRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal retry actionCreate params: %v", err)
	}
	retryCreateAny, rpcErr := h.actionCreate(ctx, retryCreateRaw)
	if rpcErr != nil {
		t.Fatalf("retry actionCreate rpc error: %+v", rpcErr)
	}
	retryCreateResp, ok := retryCreateAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected retry actionCreate response type %T", retryCreateAny)
	}
	retryActionID, ok := retryCreateResp["action_id"].(string)
	if !ok || retryActionID == "" || retryActionID == failedActionID {
		t.Fatalf("unexpected retry actionCreate response %+v", retryCreateResp)
	}

	retryAction, err := store.GetHumanAction(ctx, retryActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", retryActionID, err)
	}
	if retryAction.AssignedTo != "reviewer-b" {
		t.Fatalf("retry action assigned_to = %q, want reviewer-b", retryAction.AssignedTo)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitResolution)

	staleStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-a",
		Comment:   "old holder should not start handed-off anomaly retry",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, staleStartRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionStart after anomaly handoff, got %+v", rpcErr)
	}

	retryStartRaw, err := json.Marshal(actionStartParams{
		ActionID:  retryActionID,
		StartedBy: "reviewer-b",
		Comment:   "new holder starts the handed-off anomaly retry",
	})
	if err != nil {
		t.Fatalf("marshal retry actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, retryStartRaw); rpcErr != nil {
		t.Fatalf("retry actionStart rpc error: %+v", rpcErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	staleResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "old holder should not resolve handed-off anomaly retry",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal stale retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, staleResolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "assigned to reviewer-b") {
		t.Fatalf("expected holder mismatch on retry actionResolve after anomaly handoff, got %+v", rpcErr)
	}

	retryResolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   retryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "new holder completed the handed-off anomaly retry",
		ResolvedBy: "reviewer-b",
	})
	if err != nil {
		t.Fatalf("marshal retry actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, retryResolveRaw); rpcErr != nil {
		t.Fatalf("retry actionResolve rpc error: %+v", rpcErr)
	}

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, retryActionID, sourceQueue.QueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)

	completedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get completed source queue after handoff: %v", err)
	}
	if completedSourceQueue.Status != "RESOLVED" || completedSourceQueue.AssignedTo != "reviewer-b" {
		t.Fatalf("completed source queue after handed-off anomaly retry = %+v", completedSourceQueue)
	}
	completedPayload, err := actionCreateDecodeQueuePayload(completedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode completed source queue payload: %v", err)
	}
	if completedPayload.ActionID != retryActionID || completedPayload.ActionAssignedTo != "reviewer-b" {
		t.Fatalf("completed payload should mirror winning handed-off anomaly retry, got %+v", completedPayload)
	}
	if completedPayload.LastFailedActionID != failedActionID {
		t.Fatalf("completed payload should preserve original anomaly failed attempt lineage, got %+v", completedPayload)
	}
	if completedPayload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("completed payload rollback_reason = %q, want verifier_late_fail", completedPayload.RollbackReason)
	}
}

func TestProcessRSPEventVerifierPressureRejectsSecondRetryPromotionAfterRollback(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-late-fail-retry-single-authority"
		taskID      = "task-rsp-rebase-late-fail-retry-single-authority"
		agentID     = "agent-rsp-rebase-late-fail-retry-single-authority"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-late-fail-retry-single-authority"
		repairID    = "tens-repair-rsp-late-fail-retry-single-authority"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail single-authority test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-late-fail-retry-single-authority",
		"fork_tension_id":     "tens-fork-rsp-late-fail-retry-single-authority",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for verifier pressure single-authority test",
		Details:           "Coalition ID: coal-rsp-late-fail-retry-single-authority\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	failedActionID, ok := createResp["action_id"].(string)
	if !ok || failedActionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  failedActionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before verifier pressure single-authority test.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-late-fail-retry-single-authority",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-late-fail-retry-single-authority","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, failedActionID, sourceQueue.QueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	assertSecondRetryPromotionFailsClosed(t, ctx, store, h, workspaceID, failedActionID, sourceQueue.QueueID)
}

func TestProcessRSPEventVerifierPressureDoesNotRollbackPausedRebaseAction(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-paused-guard"
		taskID      = "task-rsp-rebase-paused-guard"
		agentID     = "agent-rsp-rebase-paused-guard"
		repairID    = "tens-repair-rsp-paused-guard"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail paused guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	pauseRaw, err := json.Marshal(actionPauseParams{
		ActionID: actionID,
		PausedBy: "reviewer-a",
		Comment:  "Pause before verifier-pressure anomaly arrives.",
	})
	if err != nil {
		t.Fatalf("marshal actionPause params: %v", err)
	}
	if _, rpcErr := h.actionPause(ctx, pauseRaw); rpcErr != nil {
		t.Fatalf("actionPause rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-paused-guard",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-paused-guard","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
}

func TestProcessRSPEventVerifierPressureDoesNotRollbackResolvedRebaseAction(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-resolved-guard"
		taskID      = "task-rsp-rebase-resolved-guard"
		agentID     = "agent-rsp-rebase-resolved-guard"
		repairID    = "tens-repair-rsp-resolved-guard"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail resolved guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}
	resolveRaw, err := json.Marshal(actionResolveParams{
		ActionID:   actionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Resolved before verifier-pressure anomaly arrives.",
		ResolvedBy: "reviewer-a",
	})
	if err != nil {
		t.Fatalf("marshal actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRaw); rpcErr != nil {
		t.Fatalf("actionResolve rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-resolved-guard",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-resolved-guard","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusCompleted, rebaseWorkflowStateCompleted, rebaseWorkflowStepActionResolved)
}

func TestProcessRSPEventVerifierPressureFailsClosedWhenCurrentCarrierBecomesUnprovable(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-rollback-recovery"
		taskID      = "task-rsp-rebase-rollback-recovery"
		agentID     = "agent-rsp-rebase-rollback-recovery"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-rollback-recovery"
		repairID    = "tens-repair-rsp-rollback-recovery"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail rollback recovery queue",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-rollback-recovery",
		"fork_tension_id":     "tens-fork-rsp-rollback-recovery",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for rollback recovery queue",
		Details:           "Coalition ID: coal-rsp-rollback-recovery\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before anomaly rollback failure test.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	corruptActionQueueSourceLinkForControlPlaneTest(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID)

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-rollback-recovery",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-rollback-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueue.QueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-rebase-rollback-recovery",
		EntityID:     repairID,
	})
	if recoveryQueue.Status != "OPEN" {
		t.Fatalf("unexpected recovery queue %+v", recoveryQueue)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.FailureTrigger != "verifier_late_fail_current_carrier_unprovable" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.EntityID != repairID || payload.Family != "verifier_pressure" {
		t.Fatalf("unexpected anomaly recovery payload %+v", payload)
	}
	if payload.ActionID != "" || payload.SourceQueueID != "" {
		t.Fatalf("expected fail-closed anomaly recovery to stay queue-less when current carrier is unprovable, got %+v", payload)
	}
}

func TestProcessRSPEventVerifierPressureQueuesRollbackFailureRecoveryUsesCurrentActionCarrierInsteadOfStaleScannedQueue(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-stale-scan-recovery"
		taskID      = "task-rsp-rebase-stale-scan-recovery"
		agentID     = "agent-rsp-rebase-stale-scan-recovery"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-stale-scan-recovery"
		repairID    = "tens-repair-rsp-stale-scan-recovery"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail stale carrier recovery",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-stale-scan-recovery",
		"fork_tension_id":     "tens-fork-rsp-stale-scan-recovery",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	staleSourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for stale carrier recovery",
		Details:           "Coalition ID: coal-rsp-stale-scan-recovery\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create stale rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     staleSourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before stale scan recovery test.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	startedStaleSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, staleSourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get started stale source queue: %v", err)
	}
	stalePayload, err := actionCreateDecodeQueuePayload(startedStaleSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode started stale source queue payload: %v", err)
	}
	currentPayloadJSON, err := json.Marshal(stalePayload)
	if err != nil {
		t.Fatalf("marshal current carrier payload: %v", err)
	}
	currentSourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "followup-current:" + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Current bounded overlap rebase",
		Summary:           "Current carrier for stale anomaly scan recovery",
		Details:           "Current linked rebase follow-up carrier for stale anomaly scan recovery.",
		PayloadJSON:       string(currentPayloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create current rebase follow-up queue: %v", err)
	}

	repointActionQueueSourceLinkForServerTest(t, ctx, store, workspaceID, actionID, currentSourceQueue.QueueID, currentSourceQueue.QueueKey)
	if _, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueID:     staleSourceQueue.QueueID,
		Status:      "RESOLVED",
		ResolvedBy:  "reviewer-a",
		Resolution:  "stale_scanned_alias",
	}); err != nil {
		t.Fatalf("resolve stale scanned source queue: %v", err)
	}

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return []sqlite.OperatorQueueRecord{startedStaleSourceQueue}, nil
	}
	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		interleaved := interleaveOperatorQueueRevisionForTest(t, ctx, store, workspaceID, currentSourceQueue.QueueID, "rsp-stale-scan-current-carrier")
		if interleaved.UpdatedAt == "" {
			hookErr = fmt.Errorf("interleaved current source queue revision did not produce updated_at")
		}
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-stale-scan-recovery",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-stale-scan-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if hookErr != nil {
		t.Fatalf("interleaving current source queue hook: %v", hookErr)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q when anomaly rollback recovery falls back", action.Status, humanActionStatusPending)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-rebase-stale-scan-recovery",
		EntityID:     repairID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_stale_scanned_carrier" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_stale_scanned_carrier", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.SourceQueueID != "" || recoveryPayload.ActionID != "" {
		t.Fatalf("stale scanned carrier recovery should stay anomaly-scoped, got %+v", recoveryPayload)
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: currentSourceQueue.QueueID,
	})); err == nil {
		t.Fatalf("expected stale scanned anomaly path to avoid queue-scoped rollback recovery linkage")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected stale recovery queue to stay absent, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureQueuesRollbackFailureRecoveryWhenQueueListFails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-list-recovery"
		taskID      = "task-rsp-rebase-list-recovery"
		agentID     = "agent-rsp-rebase-list-recovery"
		entityID    = "tens-repair-rsp-list-recovery"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail list recovery queue",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return nil, errors.New("forced operator queue list failure")
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-list-recovery",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    entityID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + entityID + `","cluster_id":"coal-rsp-list-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-rebase-list-recovery",
		EntityID:     entityID,
	})
	if recoveryQueue.Status != "OPEN" {
		t.Fatalf("recovery queue status = %q, want OPEN", recoveryQueue.Status)
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if payload.FailureScope != "rsp_anomaly_list" || payload.FailureTrigger != "verifier_late_fail_queue_list" {
		t.Fatalf("unexpected recovery payload %+v", payload)
	}
	if payload.EntityID != entityID || payload.Family != "verifier_pressure" {
		t.Fatalf("unexpected list recovery payload %+v", payload)
	}
	if payload.SourceQueueID != "" || payload.ActionID != "" || payload.RepairTensionID != "" {
		t.Fatalf("expected list recovery queue to stay workspace/entity scoped, got %+v", payload)
	}
}

func TestProcessRSPEventVerifierPressureRollbackDiscoveryRequestsUnboundedFollowupScan(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-scan-window-guard"
		taskID      = "task-rsp-rebase-scan-window-guard"
		agentID     = "agent-rsp-rebase-scan-window-guard"
		repairID    = "tens-repair-rsp-scan-window-guard"
	)

	createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	var observed sqlite.OperatorQueueFilter
	h.listOperatorQueueItemsOverride = func(_ context.Context, filter sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		observed = filter
		return nil, nil
	}

	h.rollbackLinkedRebaseFollowupsForAnomaly(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-scan-window-guard",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
	}, repairID, "verifier_pressure")

	if observed.WorkspaceID != workspaceID {
		t.Fatalf("workspace filter = %q, want %q", observed.WorkspaceID, workspaceID)
	}
	if observed.QueueType != "FOLLOW_UP" {
		t.Fatalf("queue type filter = %q, want FOLLOW_UP", observed.QueueType)
	}
	if observed.Status != "OPEN" {
		t.Fatalf("status filter = %q, want OPEN", observed.Status)
	}
	if observed.Limit != -1 {
		t.Fatalf("limit = %d, want -1 for unbounded anomaly discovery", observed.Limit)
	}
}

func TestProcessRSPEventVerifierPressureRejectsAmbiguousLinkedRebaseCarriers(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-ambiguous-carriers"
		taskID      = "task-rsp-rebase-ambiguous-carriers"
		agentID     = "agent-rsp-rebase-ambiguous-carriers"
		repairID    = "tens-repair-rsp-ambiguous-carriers"
	)

	firstActionID, _ := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail ambiguity guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	secondPayload, err := json.Marshal(map[string]any{
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "duplicate_carrier_noise",
	})
	if err != nil {
		t.Fatalf("marshal second queue payload: %v", err)
	}
	secondQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "manual_rebase_followup_duplicate:" + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Duplicate rebase follow-up carrier",
		Summary:           "Second carrier for anomaly ambiguity guard.",
		Details:           "Synthetic duplicate carrier for anomaly ambiguity guard.",
		PayloadJSON:       string(secondPayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert second queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     secondQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal second actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("second actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected second actionCreate response type %T", createAny)
	}
	secondActionID, ok := createResp["action_id"].(string)
	if !ok || secondActionID == "" {
		t.Fatalf("unexpected second actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  secondActionID,
		StartedBy: "reviewer-a",
		Comment:   "Start duplicate carrier for ambiguity coverage.",
	})
	if err != nil {
		t.Fatalf("marshal second actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("second actionStart rpc error: %+v", rpcErr)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-ambiguous-carriers",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-ambiguous-carriers","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	firstAction, err := store.GetHumanAction(ctx, firstActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", firstActionID, err)
	}
	if firstAction.Status != humanActionStatusPending {
		t.Fatalf("first action status = %q, want %q", firstAction.Status, humanActionStatusPending)
	}
	secondAction, err := store.GetHumanAction(ctx, secondActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", secondActionID, err)
	}
	if secondAction.Status != humanActionStatusPending {
		t.Fatalf("second action status = %q, want %q", secondAction.Status, humanActionStatusPending)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-rebase-ambiguous-carriers",
		EntityID:     repairID,
	})
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode ambiguity recovery payload: %v", err)
	}
	if payload.SourceQueueID != "" || payload.ActionID != "" {
		t.Fatalf("expected ambiguity recovery to stay anomaly-scoped, got %+v", payload)
	}
	if !strings.Contains(strings.ToLower(payload.FailureMessage), "multiple active linked rebase carriers") {
		t.Fatalf("unexpected ambiguity recovery payload %+v", payload)
	}
}

func TestProcessRSPEventVerifierPressureRejectsDuplicateCarrierRowsForSameAction(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-duplicate-carriers-same-action"
		taskID      = "task-rsp-rebase-duplicate-carriers-same-action"
		agentID     = "agent-rsp-rebase-duplicate-carriers-same-action"
		repairID    = "tens-repair-rsp-duplicate-carriers-same-action"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable duplicate-carrier ambiguity guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	var duplicatePayload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(sourceQueue.PayloadJSON), &duplicatePayload); err != nil {
		t.Fatalf("decode source queue payload for duplicate carrier: %v", err)
	}
	duplicatePayload.DecisionReason = "duplicate_carrier_same_action"
	duplicatePayload.SourceQueueID = ""
	duplicatePayload.SourceQueueKey = ""
	duplicatePayload.Normalize()
	duplicatePayloadJSON, err := json.Marshal(duplicatePayload)
	if err != nil {
		t.Fatalf("marshal duplicate carrier payload: %v", err)
	}
	duplicateQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          "manual_rebase_followup_duplicate_same_action:" + repairID,
		QueueType:         "FOLLOW_UP",
		Title:             "Duplicate same-action rebase carrier",
		Summary:           "Synthetic duplicate carrier with same action linkage.",
		Details:           "Duplicate carrier for same-action ambiguity guard.",
		PayloadJSON:       string(duplicatePayloadJSON),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert duplicate same-action queue: %v", err)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-duplicate-carriers-same-action",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-duplicate-carriers-same-action","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q", action.Status, humanActionStatusPending)
	}

	sourceQueueAfter, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueueAfter.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN", sourceQueueAfter.Status)
	}
	latestDuplicateQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, duplicateQueue.QueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", duplicateQueue.QueueID, err)
	}
	if latestDuplicateQueue.Status != "OPEN" {
		t.Fatalf("duplicate queue status = %q, want OPEN", latestDuplicateQueue.Status)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-rebase-duplicate-carriers-same-action",
		EntityID:     repairID,
	})
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode duplicate same-action recovery payload: %v", err)
	}
	if payload.SourceQueueID != "" || payload.ActionID != "" {
		t.Fatalf("expected duplicate same-action ambiguity recovery to stay anomaly-scoped, got %+v", payload)
	}
	if payload.FailureTrigger != "verifier_late_fail_ambiguous_carriers" && payload.FailureTrigger != "verifier_late_fail_stale_scanned_carrier" {
		t.Fatalf("unexpected duplicate same-action recovery trigger %+v", payload)
	}
	if !strings.Contains(strings.ToLower(payload.FailureMessage), "multiple active linked rebase carriers") &&
		!strings.Contains(strings.ToLower(payload.FailureMessage), "no longer matches current action carrier") {
		t.Fatalf("unexpected duplicate same-action ambiguity recovery payload %+v", payload)
	}
}

func TestProcessRSPEventVerifierPressureRejectsStaleScannedCarrierWhenActionQueuePointsElsewhere(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID     = "ws-rsp-rebase-stale-scanned-carrier"
		taskID          = "task-rsp-rebase-stale-scanned-carrier"
		agentID         = "agent-rsp-rebase-stale-scanned-carrier"
		repairID        = "tens-repair-rsp-stale-scanned-carrier"
		otherRepairID   = "tens-repair-rsp-stale-scanned-carrier-other"
		otherCarrierKey = "manual_rebase_followup_current_elsewhere:tens-repair-rsp-stale-scanned-carrier-other"
		eventID         = "evt-rsp-rebase-stale-scanned-carrier"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable stale scanned carrier guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	otherPayload, err := json.Marshal(map[string]any{
		"repair_tension_id":   otherRepairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "current_action_carrier_moved_elsewhere",
	})
	if err != nil {
		t.Fatalf("marshal other current carrier payload: %v", err)
	}
	otherQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          otherCarrierKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Current carrier moved elsewhere",
		Summary:           "Synthetic current carrier for stale scanned anomaly guard.",
		Details:           "Action queue now points at a different source queue.",
		PayloadJSON:       string(otherPayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          otherRepairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("upsert alternate current carrier: %v", err)
	}

	repointActionQueueSourceLinkForServerTest(t, ctx, store, workspaceID, actionID, otherQueue.QueueID, otherQueue.QueueKey)

	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-stale-scanned-carrier","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q when scanned carrier is stale", action.Status, humanActionStatusPending)
	}

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if sourceQueue.Status != "OPEN" {
		t.Fatalf("stale scanned source queue status = %q, want OPEN", sourceQueue.Status)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     repairID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode stale scanned carrier recovery payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_stale_scanned_carrier" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_stale_scanned_carrier", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.ActionID != "" || recoveryPayload.SourceQueueID != "" {
		t.Fatalf("expected stale scanned carrier recovery to stay anomaly-scoped, got %+v", recoveryPayload)
	}
	if !strings.Contains(strings.ToLower(recoveryPayload.FailureMessage), "no longer matches current action carrier") {
		t.Fatalf("unexpected stale scanned carrier recovery payload %+v", recoveryPayload)
	}
}

func TestCurrentPendingLinkedRebaseCandidatesForAnomalyUsesCurrentActionCarrier(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID     = "ws-rsp-current-anomaly-carrier"
		taskID          = "task-rsp-current-anomaly-carrier"
		agentID         = "agent-rsp-current-anomaly-carrier"
		repairID        = "tens-repair-rsp-current-anomaly-carrier"
		otherCarrierKey = "manual_rebase_followup_current_action_carrier:tens-repair-rsp-current-anomaly-carrier"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	var currentPayload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(sourceQueue.PayloadJSON), &currentPayload); err != nil {
		t.Fatalf("decode current source queue payload: %v", err)
	}
	currentPayload.DecisionReason = "current_action_carrier_recheck"
	currentPayload.Normalize()
	currentPayloadJSON, err := json.Marshal(currentPayload)
	if err != nil {
		t.Fatalf("marshal current action carrier payload: %v", err)
	}
	otherQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          otherCarrierKey,
		QueueType:         sourceQueue.QueueType,
		Title:             sourceQueue.Title,
		Summary:           sourceQueue.Summary,
		Details:           sourceQueue.Details,
		PayloadJSON:       string(currentPayloadJSON),
		AssignedTo:        sourceQueue.AssignedTo,
		Urgency:           sourceQueue.Urgency,
		SourceKind:        sourceQueue.SourceKind,
		SourceID:          sourceQueue.SourceID,
		TaskID:            sourceQueue.TaskID,
		SessionID:         sourceQueue.SessionID,
		AgentID:           sourceQueue.AgentID,
		KeepSessionActive: sourceQueue.KeepSessionActive,
	})
	if err != nil {
		t.Fatalf("upsert alternate current action carrier: %v", err)
	}
	repointActionQueueSourceLinkForServerTest(t, ctx, store, workspaceID, actionID, otherQueue.QueueID, otherQueue.QueueKey)

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return []sqlite.OperatorQueueRecord{sourceQueue}, nil
	}

	candidates, err := h.currentPendingLinkedRebaseCandidatesForAnomaly(ctx, workspaceID, repairID)
	if err != nil {
		t.Fatalf("currentPendingLinkedRebaseCandidatesForAnomaly: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(candidates))
	}
	if candidates[0].actionID != actionID {
		t.Fatalf("candidate action_id = %q, want %q", candidates[0].actionID, actionID)
	}
	if candidates[0].item.QueueID != otherQueue.QueueID {
		t.Fatalf("candidate queue_id = %q, want current action carrier %q", candidates[0].item.QueueID, otherQueue.QueueID)
	}
	if candidates[0].item.QueueKey != otherQueue.QueueKey {
		t.Fatalf("candidate queue_key = %q, want current action carrier %q", candidates[0].item.QueueKey, otherQueue.QueueKey)
	}
}

func TestCurrentPendingLinkedRebaseCandidatesForAnomalyRejectsScannedFallbackWhenActionPointsToDifferentEntity(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID     = "ws-rsp-current-anomaly-carrier-drift"
		taskID          = "task-rsp-current-anomaly-carrier-drift"
		agentID         = "agent-rsp-current-anomaly-carrier-drift"
		repairID        = "tens-repair-rsp-current-anomaly-carrier-drift"
		otherRepairID   = "tens-repair-rsp-current-anomaly-carrier-drift-other"
		otherCarrierKey = "manual_rebase_followup_current_action_carrier_drift:tens-repair-rsp-current-anomaly-carrier-drift-other"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	var currentPayload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(sourceQueue.PayloadJSON), &currentPayload); err != nil {
		t.Fatalf("decode current source queue payload: %v", err)
	}
	currentPayload.RepairTensionID = otherRepairID
	currentPayload.Normalize()
	currentPayloadJSON, err := json.Marshal(currentPayload)
	if err != nil {
		t.Fatalf("marshal current action carrier drift payload: %v", err)
	}
	otherQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          otherCarrierKey,
		QueueType:         sourceQueue.QueueType,
		Title:             sourceQueue.Title,
		Summary:           sourceQueue.Summary,
		Details:           sourceQueue.Details,
		PayloadJSON:       string(currentPayloadJSON),
		AssignedTo:        sourceQueue.AssignedTo,
		Urgency:           sourceQueue.Urgency,
		SourceKind:        sourceQueue.SourceKind,
		SourceID:          otherRepairID,
		TaskID:            sourceQueue.TaskID,
		SessionID:         sourceQueue.SessionID,
		AgentID:           sourceQueue.AgentID,
		KeepSessionActive: sourceQueue.KeepSessionActive,
	})
	if err != nil {
		t.Fatalf("upsert alternate different-entity carrier: %v", err)
	}
	repointActionQueueSourceLinkForServerTest(t, ctx, store, workspaceID, actionID, otherQueue.QueueID, otherQueue.QueueKey)

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return []sqlite.OperatorQueueRecord{sourceQueue}, nil
	}

	candidates, err := h.currentPendingLinkedRebaseCandidatesForAnomaly(ctx, workspaceID, repairID)
	if err != nil {
		t.Fatalf("currentPendingLinkedRebaseCandidatesForAnomaly: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidate count = %d, want 0 when current action carrier now points at a different anomaly entity", len(candidates))
	}
}

func TestCurrentPendingLinkedRebaseCandidatesForAnomalyRejectsIncidentalActionIdentityMatch(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-current-anomaly-incidental-action-id"
		taskID      = "task-rsp-current-anomaly-incidental-action-id"
		agentID     = "agent-rsp-current-anomaly-incidental-action-id"
		repairID    = "tens-repair-rsp-current-anomaly-incidental-action-id"
	)

	actionID, _ := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)

	candidates, err := h.currentPendingLinkedRebaseCandidatesForAnomaly(ctx, workspaceID, actionID)
	if err != nil {
		t.Fatalf("currentPendingLinkedRebaseCandidatesForAnomaly: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidate count = %d, want 0 when anomaly entity only matches incidental action identity", len(candidates))
	}
}

func TestProcessRSPEventVerifierPressureRejectsIncidentalActionIDEntityMatch(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-incidental-action-id-guard"
		taskID      = "task-rsp-incidental-action-id-guard"
		agentID     = "agent-rsp-incidental-action-id-guard"
		repairID    = "tens-repair-rsp-incidental-action-id-guard"
		eventID     = "evt-rsp-incidental-action-id-guard"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable incidental action-id anomaly guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	})

	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "human_action",
		EntityID:    actionID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + actionID + `","cluster_id":"coal-rsp-incidental-action-id-guard","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q when anomaly entity only matches incidental action identity", action.Status, humanActionStatusPending)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusPending, rebaseWorkflowStateInProgress, rebaseWorkflowStepOperatorClaimed)
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}); len(got) != len(seenResolvedEvents) {
		t.Fatalf("incidental action-id anomaly match should not resolve any action: before=%d after=%d", len(seenResolvedEvents), len(got))
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly",
		ActionID:     actionID,
	})); err == nil {
		t.Fatalf("expected incidental action-id anomaly path to avoid queue-scoped rollback-failure recovery")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected queue-scoped rollback-failure recovery to stay absent, got %v", err)
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     actionID,
	})); err == nil {
		t.Fatalf("expected incidental action-id anomaly path to avoid anomaly-list rollback-failure recovery")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected anomaly-list rollback-failure recovery to stay absent, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureReusesCurrentCarrierWhenScanSnapshotIsStaleAndConcurrentUpsertWins(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID    = "ws-rsp-current-carrier-manual-winner"
		taskID         = "task-rsp-current-carrier-manual-winner"
		agentID        = "agent-rsp-current-carrier-manual-winner"
		currentAgentID = "agent-rsp-current-carrier-manual-winner-current"
		repairID       = "tens-repair-rsp-current-carrier-manual-winner"
		winnerSummary  = "winner manual note should survive anomaly rollback"
		winnerDetails  = "winner workspace.ops.upsert should not force false rsp rollback-failure recovery"
		winnerDueAt    = "2099-09-03T00:00:00Z"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable current carrier metadata anomaly recovery",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	staleSnapshot, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	if staleSnapshot.AgentID != agentID {
		t.Fatalf("stale snapshot agent_id = %q, want %q", staleSnapshot.AgentID, agentID)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     currentAgentID,
		OwnerUserID: "developer",
		DisplayName: "Current Carrier Agent",
	}); err != nil {
		t.Fatalf("register current carrier agent: %v", err)
	}
	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:          workspaceID,
		QueueKey:             staleSnapshot.QueueKey,
		QueueType:            staleSnapshot.QueueType,
		Title:                staleSnapshot.Title,
		Summary:              staleSnapshot.Summary,
		Details:              staleSnapshot.Details,
		PayloadJSON:          staleSnapshot.PayloadJSON,
		AssignedTo:           staleSnapshot.AssignedTo,
		Urgency:              staleSnapshot.Urgency,
		SourceKind:           staleSnapshot.SourceKind,
		SourceID:             staleSnapshot.SourceID,
		TaskID:               staleSnapshot.TaskID,
		SessionID:            staleSnapshot.SessionID,
		AgentID:              currentAgentID,
		KeepSessionActive:    staleSnapshot.KeepSessionActive,
		RequireCurrentStatus: "OPEN",
	}); err != nil {
		t.Fatalf("refresh current carrier metadata: %v", err)
	}
	currentSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem current(%s): %v", sourceQueueID, err)
	}
	if currentSourceQueue.AgentID != currentAgentID {
		t.Fatalf("current source queue agent_id = %q, want %q", currentSourceQueue.AgentID, currentAgentID)
	}

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return []sqlite.OperatorQueueRecord{staleSnapshot}, nil
	}
	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		upsertRaw, err := json.Marshal(workspaceOpsUpsertParams{
			WorkspaceID:      workspaceID,
			QueueID:          currentSourceQueue.QueueID,
			QueueKey:         currentSourceQueue.QueueKey,
			QueueType:        currentSourceQueue.QueueType,
			Title:            currentSourceQueue.Title,
			Summary:          winnerSummary,
			Details:          winnerDetails,
			AssignedTo:       currentSourceQueue.AssignedTo,
			Urgency:          "CRITICAL",
			DueAt:            winnerDueAt,
			SourceKind:       currentSourceQueue.SourceKind,
			SourceID:         currentSourceQueue.SourceID,
			TaskID:           currentSourceQueue.TaskID,
			AgentID:          currentSourceQueue.AgentID,
			CurrentRevision:  currentSourceQueue.Revision,
			CurrentUpdatedAt: currentSourceQueue.UpdatedAt,
		})
		if err != nil {
			hookErr = fmt.Errorf("marshal interleaving workspaceOpsUpsert params: %w", err)
			return
		}
		if _, rpcErr := h.workspaceOpsUpsert(testAuthContext(workspaceID, "system", "tests"), upsertRaw); rpcErr != nil {
			hookErr = fmt.Errorf("interleaving workspaceOpsUpsert rpc error: %+v", rpcErr)
		}
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-current-carrier-metadata",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-current-carrier-metadata","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	if hookErr != nil {
		t.Fatalf("interleaving current source queue hook: %v", hookErr)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusFailed {
		t.Fatalf("action status = %q, want %q after anomaly rollback retries current carrier", action.Status, humanActionStatusFailed)
	}
	if action.ResolvedBy != "system:rsp" {
		t.Fatalf("action resolved_by = %q, want system:rsp", action.ResolvedBy)
	}
	if !strings.Contains(action.ResolutionComment, "RSP late verifier fail rollback") {
		t.Fatalf("action resolution comment did not surface anomaly rollback context: %q", action.ResolutionComment)
	}

	actionQueue := operatorQueueForSource(t, ctx, store, workspaceID, "human_action", actionID)
	if actionQueue.Status != "RESOLVED" {
		t.Fatalf("action queue status = %q, want RESOLVED", actionQueue.Status)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, currentSourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	if updatedSourceQueue.Status != "OPEN" {
		t.Fatalf("source queue status = %q, want OPEN", updatedSourceQueue.Status)
	}
	if updatedSourceQueue.AgentID != currentAgentID {
		t.Fatalf("updated source queue agent_id = %q, want %q", updatedSourceQueue.AgentID, currentAgentID)
	}
	if !strings.Contains(updatedSourceQueue.Summary, winnerSummary) {
		t.Fatalf("source queue summary should preserve winner-owned manual edit, got %q", updatedSourceQueue.Summary)
	}
	if !strings.Contains(updatedSourceQueue.Details, winnerDetails) {
		t.Fatalf("source queue details should preserve winner-owned manual edit, got %q", updatedSourceQueue.Details)
	}
	if updatedSourceQueue.Urgency != "CRITICAL" {
		t.Fatalf("source queue urgency = %q, want CRITICAL", updatedSourceQueue.Urgency)
	}
	gotDueAt := ""
	if updatedSourceQueue.DueAt != nil {
		gotDueAt = strings.TrimSpace(*updatedSourceQueue.DueAt)
	}
	if gotDueAt != winnerDueAt {
		t.Fatalf("source queue due_at = %q, want %q", gotDueAt, winnerDueAt)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.ActionID != "" {
		t.Fatalf("expected source queue active action link to clear after anomaly rollback, got %q", payload.ActionID)
	}
	if payload.LastFailedActionID != actionID {
		t.Fatalf("last_failed_action_id = %q, want %q", payload.LastFailedActionID, actionID)
	}
	if payload.RollbackReason != "verifier_late_fail" {
		t.Fatalf("rollback_reason = %q, want verifier_late_fail", payload.RollbackReason)
	}
	if payload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("payload workflow_state = %q, want %q", payload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitRestart {
		t.Fatalf("payload workflow_step = %q, want %q", payload.RebaseWorkflowStep, rebaseWorkflowStepAwaitRestart)
	}

	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly",
		ActionID:     actionID,
	})); err == nil {
		t.Fatalf("expected no queue-scoped anomaly rollback-failure recovery after current-carrier retry")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected queue-scoped anomaly rollback-failure recovery to stay absent, got %v", err)
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-current-carrier-metadata",
		EntityID:     repairID,
	})); err == nil {
		t.Fatalf("expected no queue-less anomaly rollback-failure recovery after current-carrier retry")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected queue-less anomaly rollback-failure recovery to stay absent, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureDoesNotQueueRollbackFailureRecoveryWhenConcurrentWinnerAlreadyRolledBack(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-concurrent-winner-no-recovery"
		taskID      = "task-rsp-rebase-concurrent-winner-no-recovery"
		agentID     = "agent-rsp-rebase-concurrent-winner-no-recovery"
		repairID    = "tens-repair-rsp-concurrent-winner-no-recovery"
		eventID     = "evt-rsp-rebase-concurrent-winner-no-recovery"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable concurrent anomaly rollback guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	})

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		currentAction, err := store.GetHumanAction(ctx, actionID)
		if err != nil {
			hookErr = fmt.Errorf("load current action for concurrent winner: %w", err)
			return
		}
		if _, rpcErr := h.resolveActionWithEffects(ctx, currentAction, actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent winner already rolled back this anomaly.",
			ResolvedBy: "system:rsp",
		}, actionResolveOptions{RollbackReason: "verifier_late_fail"}); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner resolve: %s", rpcErr.Message)
		}
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-concurrent-winner-no-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	if hookErr != nil {
		t.Fatalf("concurrent winner hook: %v", hookErr)
	}
	assertBlackBoxRebaseWorkflowAuthority(t, ctx, store, workspaceID, actionID, sourceQueueID, humanActionStatusFailed, rebaseWorkflowStateClaimed, rebaseWorkflowStepAwaitRestart)
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}); len(got) != len(seenResolvedEvents)+1 {
		t.Fatalf("concurrent winner should resolve action exactly once: before=%d after=%d", len(seenResolvedEvents), len(got))
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly",
		ActionID:     actionID,
	})); err == nil {
		t.Fatalf("expected concurrent winner anomaly path to avoid queue-scoped rollback-failure recovery")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected queue-scoped rollback-failure recovery to stay absent, got %v", err)
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     repairID,
	})); err == nil {
		t.Fatalf("expected concurrent winner anomaly path to avoid anomaly-list rollback-failure recovery")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected anomaly-list rollback-failure recovery to stay absent, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureConcurrentWinnerKeepsResolvedRollbackFailureQueueTerminal(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-concurrent-winner-terminal-recovery"
		taskID      = "task-rsp-rebase-concurrent-winner-terminal-recovery"
		agentID     = "agent-rsp-rebase-concurrent-winner-terminal-recovery"
		repairID    = "tens-repair-rsp-concurrent-winner-terminal-recovery"
		eventID     = "evt-rsp-rebase-concurrent-winner-terminal-recovery"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for destructive concurrent-winner terminal coverage",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("PutCapabilityPolicy: %v", err)
	}

	h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "rsp_anomaly",
		FailureTrigger:  "verifier_late_fail",
		FailureMessage:  "pre-existing rollback failure queue should stay terminal",
		TaskID:          taskID,
		AgentID:         agentID,
		SourceQueueID:   sourceQueueID,
		ActionID:        actionID,
		RepairTensionID: repairID,
		EventID:         eventID,
	})

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueueID,
	})

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     recoveryQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("rollback-failure actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected rollback-failure actionCreate response type %T", createAny)
	}
	recoveryActionID, ok := createResp["action_id"].(string)
	if !ok || recoveryActionID == "" {
		t.Fatalf("unexpected rollback-failure actionCreate response %+v", createResp)
	}

	resolveRecoveryRaw, err := json.Marshal(actionResolveParams{
		ActionID:   recoveryActionID,
		Resolution: humanActionStatusCompleted,
		Comment:    "Recovery queue already handled before concurrent anomaly loser arrives.",
		ResolvedBy: "operator:terminal-recovery",
	})
	if err != nil {
		t.Fatalf("marshal rollback-failure actionResolve params: %v", err)
	}
	if _, rpcErr := h.actionResolve(ctx, resolveRecoveryRaw); rpcErr != nil {
		t.Fatalf("rollback-failure actionResolve rpc error: %+v", rpcErr)
	}

	resolvedRecoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueueID,
	})
	if resolvedRecoveryQueue.Status != "RESOLVED" {
		t.Fatalf("resolved recovery queue status = %q, want RESOLVED", resolvedRecoveryQueue.Status)
	}
	seenRecoveryQueueEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedRecoveryQueue.QueueID,
		Limit:       20,
	})
	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	})

	var hookErr error
	h.beforeActionResolveQueueEffectsOverride = func(ctx context.Context) {
		h.beforeActionResolveQueueEffectsOverride = nil
		currentAction, err := store.GetHumanAction(ctx, actionID)
		if err != nil {
			hookErr = fmt.Errorf("load current action for concurrent winner with terminal recovery queue: %w", err)
			return
		}
		if _, rpcErr := h.resolveActionWithEffects(ctx, currentAction, actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    "Concurrent winner should not reopen terminal recovery queue.",
			ResolvedBy: "system:rsp",
		}, actionResolveOptions{RollbackReason: "verifier_late_fail"}); rpcErr != nil {
			hookErr = fmt.Errorf("concurrent winner resolve with terminal recovery queue: %s", rpcErr.Message)
		}
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-concurrent-winner-terminal-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	if hookErr != nil {
		t.Fatalf("concurrent winner hook with terminal recovery queue: %v", hookErr)
	}

	currentRecoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueueID,
	})
	if currentRecoveryQueue.Status != "RESOLVED" {
		t.Fatalf("terminal recovery queue status = %q, want RESOLVED", currentRecoveryQueue.Status)
	}
	if currentRecoveryQueue.UpdatedAt != resolvedRecoveryQueue.UpdatedAt {
		t.Fatalf("terminal recovery queue updated_at changed: got %q want %q", currentRecoveryQueue.UpdatedAt, resolvedRecoveryQueue.UpdatedAt)
	}
	if currentRecoveryQueue.Revision != resolvedRecoveryQueue.Revision {
		t.Fatalf("terminal recovery queue revision changed: got %d want %d", currentRecoveryQueue.Revision, resolvedRecoveryQueue.Revision)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    resolvedRecoveryQueue.QueueID,
		Limit:       20,
	}); len(got) != len(seenRecoveryQueueEvents) {
		t.Fatalf("concurrent anomaly loser should not append terminal recovery queue events: before=%d after=%d", len(seenRecoveryQueueEvents), len(got))
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    actionID,
		Limit:       20,
	}); len(got) != len(seenResolvedEvents)+1 {
		t.Fatalf("concurrent winner with terminal recovery queue should resolve source action exactly once: before=%d after=%d", len(seenResolvedEvents), len(got))
	}
	if _, err := store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     repairID,
	})); err == nil {
		t.Fatalf("expected concurrent winner with terminal recovery queue to avoid anomaly-list rollback-failure recovery")
	} else if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected anomaly-list rollback-failure recovery to stay absent, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureRejectsInterleavingCarrierRepointedToDifferentEntity(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID     = "ws-rsp-rebase-interleaving-repoint-different-entity"
		taskID          = "task-rsp-rebase-interleaving-repoint-different-entity"
		agentID         = "agent-rsp-rebase-interleaving-repoint-different-entity"
		repairID        = "tens-repair-rsp-interleaving-repoint-different-entity"
		otherRepairID   = "tens-repair-rsp-interleaving-repoint-different-entity-other"
		eventID         = "evt-rsp-rebase-interleaving-repoint-different-entity"
		otherCarrierKey = "manual_rebase_followup_interleaving_repoint_different_entity:tens-repair-rsp-interleaving-repoint-different-entity-other"
	)

	actionID, sourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable interleaving repoint anomaly guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	sourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", sourceQueueID, err)
	}
	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return []sqlite.OperatorQueueRecord{sourceQueue}, nil
	}

	var hookErr error
	h.beforeRSPAnomalyRollbackResolveOverride = func(ctx context.Context, seenActionID string) {
		if strings.TrimSpace(seenActionID) != actionID {
			return
		}
		h.beforeRSPAnomalyRollbackResolveOverride = nil

		var driftPayload model.RebaseFollowupPayload
		if err := json.Unmarshal([]byte(sourceQueue.PayloadJSON), &driftPayload); err != nil {
			hookErr = fmt.Errorf("decode source queue payload for repoint: %w", err)
			return
		}
		driftPayload.RepairTensionID = otherRepairID
		driftPayload.Normalize()
		driftPayloadJSON, err := json.Marshal(driftPayload)
		if err != nil {
			hookErr = fmt.Errorf("marshal repointed carrier payload: %w", err)
			return
		}
		otherQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
			WorkspaceID:       workspaceID,
			QueueKey:          otherCarrierKey,
			QueueType:         sourceQueue.QueueType,
			Title:             sourceQueue.Title,
			Summary:           sourceQueue.Summary,
			Details:           sourceQueue.Details,
			PayloadJSON:       string(driftPayloadJSON),
			AssignedTo:        sourceQueue.AssignedTo,
			Urgency:           sourceQueue.Urgency,
			SourceKind:        sourceQueue.SourceKind,
			SourceID:          otherRepairID,
			TaskID:            sourceQueue.TaskID,
			SessionID:         sourceQueue.SessionID,
			AgentID:           sourceQueue.AgentID,
			KeepSessionActive: sourceQueue.KeepSessionActive,
		})
		if err != nil {
			hookErr = fmt.Errorf("upsert repointed different-entity carrier: %w", err)
			return
		}
		repointActionQueueSourceLinkForServerTest(t, ctx, store, workspaceID, actionID, otherQueue.QueueID, otherQueue.QueueKey)
	}

	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		Limit:       20,
	})
	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-interleaving-repoint-different-entity","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	if hookErr != nil {
		t.Fatalf("interleaving repoint hook: %v", hookErr)
	}
	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q after interleaving repoint to different entity", action.Status, humanActionStatusPending)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		Limit:       20,
	}); len(got) != len(seenResolvedEvents) {
		t.Fatalf("interleaving repoint should not resolve any action: before=%d after=%d", len(seenResolvedEvents), len(got))
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     repairID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode interleaving repoint recovery payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_interleaving_ambiguous_carriers" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_interleaving_ambiguous_carriers", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.ActionID != "" || recoveryPayload.SourceQueueID != "" {
		t.Fatalf("expected interleaving repoint recovery to stay anomaly-scoped, got %+v", recoveryPayload)
	}
}

func TestProcessRSPEventVerifierPressureRejectsInterleavingAmbiguousCarrierAfterUniqueScan(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-interleaving-ambiguous-carrier"
		taskID      = "task-rsp-rebase-interleaving-ambiguous-carrier"
		agentID     = "agent-rsp-rebase-interleaving-ambiguous-carrier"
		repairID    = "tens-repair-rsp-interleaving-ambiguous-carrier"
		eventID     = "evt-rsp-rebase-interleaving-ambiguous-carrier"
	)

	firstActionID, firstSourceQueueID := createStartedRebaseFollowupActionForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable interleaving anomaly guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	secondActionID := ""
	secondSourceQueueID := ""
	h.beforeRSPAnomalyRollbackResolveOverride = func(ctx context.Context, actionID string) {
		if strings.TrimSpace(actionID) != firstActionID {
			return
		}
		h.beforeRSPAnomalyRollbackResolveOverride = nil
		secondActionID, secondSourceQueueID = createStartedRebaseFollowupActionOnExistingWorkspaceForControlPlaneTest(t, ctx, store, h, workspaceID, taskID, agentID, repairID)
	}

	seenResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		Limit:       20,
	})
	h.processRSPEvent(ctx, EventMessage{
		EventID:     eventID,
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-interleaving-ambiguous-carrier","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	firstAction, err := store.GetHumanAction(ctx, firstActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", firstActionID, err)
	}
	if firstAction.Status != humanActionStatusPending {
		t.Fatalf("first action status = %q, want %q", firstAction.Status, humanActionStatusPending)
	}
	if secondActionID == "" || secondSourceQueueID == "" {
		t.Fatalf("expected interleaving hook to create second carrier")
	}
	secondAction, err := store.GetHumanAction(ctx, secondActionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", secondActionID, err)
	}
	if secondAction.Status != humanActionStatusPending {
		t.Fatalf("second action status = %q, want %q", secondAction.Status, humanActionStatusPending)
	}
	firstSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, firstSourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", firstSourceQueueID, err)
	}
	if firstSourceQueue.Status != "OPEN" {
		t.Fatalf("first source queue status = %q, want OPEN", firstSourceQueue.Status)
	}
	secondSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, secondSourceQueueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", secondSourceQueueID, err)
	}
	if secondSourceQueue.Status != "OPEN" {
		t.Fatalf("second source queue status = %q, want OPEN", secondSourceQueue.Status)
	}
	if got := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		Limit:       20,
	}); len(got) != len(seenResolvedEvents) {
		t.Fatalf("interleaving ambiguity should not resolve any action: before=%d after=%d", len(seenResolvedEvents), len(got))
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      eventID,
		EntityID:     repairID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode interleaving ambiguity recovery payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_interleaving_ambiguous_carriers" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_interleaving_ambiguous_carriers", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.ActionID != "" || recoveryPayload.SourceQueueID != "" {
		t.Fatalf("expected interleaving ambiguity recovery to stay anomaly-scoped, got %+v", recoveryPayload)
	}
}

func TestProcessRSPEventVerifierPressureQueueListRecoverySeparatesDistinctEventIDs(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-queue-list-event-scope"
		taskID      = "task-rsp-rebase-queue-list-event-scope"
		agentID     = "agent-rsp-rebase-queue-list-event-scope"
		entityID    = "tens-repair-rsp-queue-list-event-scope"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable event-scoped anomaly list recovery",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	h.listOperatorQueueItemsOverride = func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
		return nil, errors.New("forced operator queue list failure")
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-anomaly-list-scope-a",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    entityID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + entityID + `","cluster_id":"coal-rsp-list-scope-a","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})
	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-anomaly-list-scope-b",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    entityID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + entityID + `","cluster_id":"coal-rsp-list-scope-b","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	firstQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-anomaly-list-scope-a",
		EntityID:     entityID,
	})
	secondQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:  workspaceID,
		FailureScope: "rsp_anomaly_list",
		EventID:      "evt-rsp-anomaly-list-scope-b",
		EntityID:     entityID,
	})

	if firstQueue.QueueID == secondQueue.QueueID {
		t.Fatalf("expected distinct anomaly-list recovery queues for distinct event ids, got %+v and %+v", firstQueue, secondQueue)
	}

	var firstPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(firstQueue.PayloadJSON), &firstPayload); err != nil {
		t.Fatalf("decode first event-scoped recovery payload: %v", err)
	}
	var secondPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(secondQueue.PayloadJSON), &secondPayload); err != nil {
		t.Fatalf("decode second event-scoped recovery payload: %v", err)
	}
	if firstPayload.EventID != "evt-rsp-anomaly-list-scope-a" || secondPayload.EventID != "evt-rsp-anomaly-list-scope-b" {
		t.Fatalf("unexpected event-scoped recovery payloads first=%+v second=%+v", firstPayload, secondPayload)
	}
}

func TestProcessRSPEventVerifierPressureQueuesRollbackFailureRecoveryWhenLinkedActionLoadFails(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-action-lookup-recovery"
		taskID      = "task-rsp-rebase-action-lookup-recovery"
		agentID     = "agent-rsp-rebase-action-lookup-recovery"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-action-lookup-recovery"
		repairID    = "tens-repair-rsp-action-lookup-recovery"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail discovery recovery queue",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-action-lookup-recovery",
		"fork_tension_id":     "tens-fork-rsp-action-lookup-recovery",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for action lookup recovery",
		Details:           "Coalition ID: coal-rsp-action-lookup-recovery\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before anomaly discovery failure test.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get started source queue: %v", err)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode started source queue payload: %v", err)
	}
	payload.ActionID = "act-missing-rsp-late-fail"
	payload.ActionQueueKey = "action:" + payload.ActionID
	payload.Normalize()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal broken source queue payload: %v", err)
	}
	writeOperatorQueuePayloadForRSPHandoffTest(t, ctx, store, workspaceID, sourceQueue.QueueID, string(payloadJSON))

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-action-lookup-recovery",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-action-lookup-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q when linked action lookup fails", action.Status, humanActionStatusPending)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		FailureScope:    "rsp_anomaly",
		RepairTensionID: repairID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_action_lookup" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_action_lookup", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.ActionID != "" || recoveryPayload.SourceQueueID != "" {
		t.Fatalf("expected repair-backed recovery to avoid stale action/source linkage, got %+v", recoveryPayload)
	}
	if recoveryPayload.RepairTensionID != repairID || recoveryPayload.EntityID != repairID || recoveryPayload.Family != "verifier_pressure" {
		t.Fatalf("unexpected recovery payload %+v", recoveryPayload)
	}
}

func TestProcessRSPEventVerifierPressureQueuesRollbackFailureRecoveryWhenCandidatePayloadDecodeFails(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-payload-decode-recovery"
		taskID      = "task-rsp-rebase-payload-decode-recovery"
		agentID     = "agent-rsp-rebase-payload-decode-recovery"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-payload-decode-recovery"
		repairID    = "tens-repair-rsp-payload-decode-recovery"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail payload recovery queue",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-payload-decode-recovery",
		"fork_tension_id":     "tens-fork-rsp-payload-decode-recovery",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for payload decode recovery",
		Details:           "Coalition ID: coal-rsp-payload-decode-recovery\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	startRaw, err := json.Marshal(actionStartParams{
		ActionID:  actionID,
		StartedBy: "reviewer-a",
		Comment:   "Begin rebase before malformed payload test.",
	})
	if err != nil {
		t.Fatalf("marshal actionStart params: %v", err)
	}
	if _, rpcErr := h.actionStart(ctx, startRaw); rpcErr != nil {
		t.Fatalf("actionStart rpc error: %+v", rpcErr)
	}

	writeOperatorQueuePayloadForRSPHandoffTest(t, ctx, store, workspaceID, sourceQueue.QueueID, `{"repair_tension_id":"`+repairID+`","action_id":"`+actionID)

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-payload-decode-recovery",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-payload-decode-recovery","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q when source queue payload decode fails", action.Status, humanActionStatusPending)
	}

	recoveryQueue := mustRebaseRollbackFailureQueueForTest(t, ctx, store, rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueue.QueueID,
	})
	var recoveryPayload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(recoveryQueue.PayloadJSON), &recoveryPayload); err != nil {
		t.Fatalf("decode recovery queue payload: %v", err)
	}
	if recoveryPayload.FailureTrigger != "verifier_late_fail_payload_decode" {
		t.Fatalf("failure_trigger = %q, want verifier_late_fail_payload_decode", recoveryPayload.FailureTrigger)
	}
	if recoveryPayload.ActionID != "" || recoveryPayload.RepairTensionID != "" {
		t.Fatalf("expected malformed payload recovery to omit decoded linkage, got %+v", recoveryPayload)
	}
	if recoveryPayload.SourceQueueID != sourceQueue.QueueID || recoveryPayload.SourceQueueKey != sourceQueue.QueueKey {
		t.Fatalf("unexpected recovery queue identity %+v", recoveryPayload)
	}
	if recoveryPayload.EntityID != repairID || recoveryPayload.Family != "verifier_pressure" {
		t.Fatalf("unexpected anomaly recovery payload %+v", recoveryPayload)
	}
}

func TestProcessRSPEventVerifierPressureDoesNotQueueRollbackFailureRecoveryForMalformedNonCandidateQueue(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-payload-decode-guard"
		taskID      = "task-rsp-rebase-payload-decode-guard"
		agentID     = "agent-rsp-rebase-payload-decode-guard"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-payload-decode-guard"
		sourceID    = "tens-repair-rsp-payload-decode-guard"
		entityID    = "tens-repair-rsp-payload-decode-target"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable malformed non-candidate guard",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Malformed rebase candidate",
		Summary:           "Should not queue anomaly rollback recovery without entity match",
		Details:           "Entity mismatch guard for malformed payload.",
		PayloadJSON:       `{"repair_tension_id":"` + sourceID + `"`,
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          sourceID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create malformed rebase follow-up queue: %v", err)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-payload-decode-guard",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    entityID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + entityID + `","cluster_id":"coal-rsp-payload-decode-guard","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	_, err = store.GetOperatorQueueItem(ctx, workspaceID, "", rebaseRollbackFailureQueueKey(rebaseRollbackFailureInput{
		WorkspaceID:   workspaceID,
		FailureScope:  "rsp_anomaly",
		SourceQueueID: sourceQueue.QueueID,
	}))
	if err == nil {
		t.Fatalf("expected malformed non-candidate queue to avoid rollback-failure recovery queue")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "operator queue item not found") {
		t.Fatalf("expected operator queue item not found, got %v", err)
	}
}

func TestProcessRSPEventVerifierPressureDoesNotRollbackUnstartedRebaseAction(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp-rebase-unstarted-guard"
		taskID      = "task-rsp-rebase-unstarted-guard"
		agentID     = "agent-rsp-rebase-unstarted-guard"
		queueKey    = "tension_rebase_followup:tens-repair-rsp-unstarted-guard"
		repairID    = "tens-repair-rsp-unstarted-guard"
	)

	createActionFixture(t, ctx, store, workspaceID, taskID, agentID)
	if _, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable verifier late fail rollback",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	queuePayload, err := json.Marshal(map[string]any{
		"coalition_id":        "coal-rsp-unstarted-guard",
		"fork_tension_id":     "tens-fork-rsp-unstarted-guard",
		"repair_tension_id":   repairID,
		"next_action":         "attempt_rebase",
		"rebase_plan_class":   "trim_redundancy",
		"conflict_safe_class": "rebase_candidate",
		"decision_reason":     "admission_risk",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	sourceQueue, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "FOLLOW_UP",
		Title:             "Attempt bounded overlap rebase",
		Summary:           "Rebase trim_redundancy for unstarted guard",
		Details:           "Coalition ID: coal-rsp-unstarted-guard\nRepair tension: " + repairID + "\nNext action: attempt_rebase",
		PayloadJSON:       string(queuePayload),
		AssignedTo:        "reviewer-a",
		Urgency:           "HIGH",
		SourceKind:        "tension",
		SourceID:          repairID,
		TaskID:            taskID,
		AgentID:           agentID,
		KeepSessionActive: true,
	})
	if err != nil {
		t.Fatalf("create rebase follow-up queue: %v", err)
	}

	createRaw, err := json.Marshal(actionCreateParams{
		WorkspaceID: workspaceID,
		QueueID:     sourceQueue.QueueID,
	})
	if err != nil {
		t.Fatalf("marshal actionCreate params: %v", err)
	}
	createAny, rpcErr := h.actionCreate(ctx, createRaw)
	if rpcErr != nil {
		t.Fatalf("actionCreate rpc error: %+v", rpcErr)
	}
	createResp, ok := createAny.(map[string]any)
	if !ok {
		t.Fatalf("unexpected actionCreate response type %T", createAny)
	}
	actionID, ok := createResp["action_id"].(string)
	if !ok || actionID == "" {
		t.Fatalf("unexpected actionCreate response %+v", createResp)
	}

	h.processRSPEvent(ctx, EventMessage{
		EventID:     "evt-rsp-rebase-unstarted-guard",
		Type:        "ANOMALY_ALERT",
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    repairID,
		PayloadJSON: `{"family":"verifier_pressure","entity_id":"` + repairID + `","cluster_id":"coal-rsp-unstarted-guard","actuation_class":"governed_hint","capability_flags":{"governed_hints_live":true}}`,
	})

	action, err := store.GetHumanAction(ctx, actionID)
	if err != nil {
		t.Fatalf("GetHumanAction(%s): %v", actionID, err)
	}
	if action.Status != humanActionStatusPending {
		t.Fatalf("action status = %q, want %q for unstarted verifier_pressure path", action.Status, humanActionStatusPending)
	}

	updatedSourceQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, sourceQueue.QueueID, "")
	if err != nil {
		t.Fatalf("get updated source queue: %v", err)
	}
	payload, err := actionCreateDecodeQueuePayload(updatedSourceQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("decode updated source queue payload: %v", err)
	}
	if payload.ActionID != actionID {
		t.Fatalf("source queue action_id = %q, want %q", payload.ActionID, actionID)
	}
	if payload.RollbackReason != "" {
		t.Fatalf("rollback_reason = %q, want empty for unstarted workflow", payload.RollbackReason)
	}
	if payload.RebaseWorkflowState != rebaseWorkflowStateClaimed {
		t.Fatalf("payload workflow_state = %q, want %q", payload.RebaseWorkflowState, rebaseWorkflowStateClaimed)
	}
	if payload.RebaseWorkflowStep != rebaseWorkflowStepAwaitResolution {
		t.Fatalf("payload workflow_step = %q, want %q", payload.RebaseWorkflowStep, rebaseWorkflowStepAwaitResolution)
	}
}

func writeOperatorQueuePayloadForRSPHandoffTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, queueID, payloadJSON string) {
	t.Helper()

	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(%s): %v", queueID, err)
	}
	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:          workspaceID,
		QueueKey:             queue.QueueKey,
		QueueType:            queue.QueueType,
		Title:                queue.Title,
		Summary:              queue.Summary,
		Details:              queue.Details,
		PayloadJSON:          payloadJSON,
		AssignedTo:           queue.AssignedTo,
		Urgency:              queue.Urgency,
		SourceKind:           queue.SourceKind,
		SourceID:             queue.SourceID,
		TaskID:               queue.TaskID,
		SessionID:            queue.SessionID,
		AgentID:              queue.AgentID,
		KeepSessionActive:    queue.KeepSessionActive,
		RequireCurrentStatus: "OPEN",
	}); err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(%s): %v", queueID, err)
	}
}

func repointActionQueueSourceLinkForServerTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actionID, sourceQueueID, sourceQueueKey string) {
	t.Helper()

	actionQueue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "action:"+actionID)
	if err != nil {
		t.Fatalf("GetOperatorQueueItem(action:%s): %v", actionID, err)
	}
	payload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		t.Fatalf("actionCreateDecodeQueuePayload(action:%s): %v", actionID, err)
	}
	payload.SourceQueueID = sourceQueueID
	payload.SourceQueueKey = sourceQueueKey
	payload.Normalize()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal repointed action queue payload: %v", err)
	}
	if _, _, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:          workspaceID,
		QueueKey:             actionQueue.QueueKey,
		QueueType:            actionQueue.QueueType,
		Title:                actionQueue.Title,
		Summary:              actionQueue.Summary,
		Details:              actionQueue.Details,
		PayloadJSON:          string(payloadJSON),
		AssignedTo:           actionQueue.AssignedTo,
		Urgency:              actionQueue.Urgency,
		SourceKind:           actionQueue.SourceKind,
		SourceID:             actionQueue.SourceID,
		TaskID:               actionQueue.TaskID,
		SessionID:            actionQueue.SessionID,
		AgentID:              actionQueue.AgentID,
		KeepSessionActive:    actionQueue.KeepSessionActive,
		RequireCurrentStatus: "OPEN",
	}); err != nil {
		t.Fatalf("UpsertOperatorQueueItemWithEvent(repoint action queue): %v", err)
	}
}
