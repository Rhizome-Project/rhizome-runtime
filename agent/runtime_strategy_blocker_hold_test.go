package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestShouldHoldRepeatedStrategyBlockerWhenCoordinationUnchanged(t *testing.T) {
	coordination := strategyBlockerCoordinationRecord("head123", "REJECTED")
	coordinationFingerprint := projectCoordinationStateFingerprintForTask(coordination, "root-task")
	resultFingerprint := strategyBlockerResultFingerprint(strategyBlockerResult())
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, map[string]any{"coordination": coordination})
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-alpha",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			StrategyBlockerTaskID:            "root-task",
			StrategyBlockerProjectID:         "project-subpixel",
			StrategyBlockerResultFingerprint: resultFingerprint,
			StrategyBlockerCoordFingerprint:  coordinationFingerprint,
			StrategyBlockerRecordedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	hold, err := runtime.shouldHoldRepeatedStrategyBlocker(context.Background(), strategyBlockerTask("strategy"))
	if err != nil {
		t.Fatalf("should hold: %v", err)
	}
	if !hold {
		t.Fatal("expected unchanged strategy dependency blocker to hold before another LLM turn")
	}
	if calls != 1 {
		t.Fatalf("expected one coordination lookup, got %d", calls)
	}
}

func TestProjectCoordinationStateFingerprintChangesOnBranchHeadChange(t *testing.T) {
	first := projectCoordinationStateFingerprintForTask(strategyBlockerCoordinationRecord("head123", "REJECTED"), "root-task")
	second := projectCoordinationStateFingerprintForTask(strategyBlockerCoordinationRecord("head456", "REJECTED"), "root-task")
	if first == "" || second == "" {
		t.Fatalf("fingerprints must be non-empty: first=%q second=%q", first, second)
	}
	if first == second {
		t.Fatalf("branch head change should wake strategy task, got same fingerprint %s", first)
	}
}

func TestProjectCoordinationStateFingerprintExcludesCandidateRootTaskChurn(t *testing.T) {
	firstCoordination := strategyBlockerCoordinationRecord("head123", "REJECTED")
	secondCoordination := strategyBlockerCoordinationRecord("head123", "REJECTED")
	firstCoordination.Tasks[0].UpdatedAt = "2026-05-08T02:00:00Z"
	secondCoordination.Tasks[0].UpdatedAt = "2026-05-08T02:05:00Z"
	first := projectCoordinationStateFingerprintForTask(firstCoordination, "root-task")
	second := projectCoordinationStateFingerprintForTask(secondCoordination, "root-task")
	if first == "" || second == "" || first != second {
		t.Fatalf("candidate root task churn should not wake strategy, first=%q second=%q", first, second)
	}
}

func TestProjectCoordinationStateFingerprintChangesOnSiblingTaskProgress(t *testing.T) {
	firstCoordination := strategyBlockerCoordinationRecord("head123", "REJECTED")
	secondCoordination := strategyBlockerCoordinationRecord("head123", "REJECTED")
	firstCoordination.Tasks[1].UpdatedAt = "2026-05-08T02:00:00Z"
	secondCoordination.Tasks[1].UpdatedAt = "2026-05-08T02:05:00Z"
	first := projectCoordinationStateFingerprintForTask(firstCoordination, "root-task")
	second := projectCoordinationStateFingerprintForTask(secondCoordination, "root-task")
	if first == "" || second == "" {
		t.Fatalf("fingerprints must be non-empty: first=%q second=%q", first, second)
	}
	if first == second {
		t.Fatalf("sibling task progress should wake strategy, got same fingerprint %s", first)
	}
}

func TestStrategyBlockerHoldExpiresByAge(t *testing.T) {
	if !strategyBlockerHoldExpired(time.Now().UTC().Add(-strategyBlockerHoldMaxAge-time.Second).Format(time.RFC3339Nano), time.Now().UTC()) {
		t.Fatal("expected old strategy blocker hold to expire")
	}
	if strategyBlockerHoldExpired(time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano), time.Now().UTC()) {
		t.Fatal("expected fresh strategy blocker hold to remain active")
	}
	if !strategyBlockerHoldExpired("not-a-time", time.Now().UTC()) {
		t.Fatal("malformed recorded_at should expire instead of holding forever")
	}
	if !strategyBlockerHoldExpired("", time.Now().UTC()) {
		t.Fatal("legacy empty recorded_at should expire instead of holding forever")
	}
}

func TestStrategyBlockerScopeDoesNotHoldImplementationTasks(t *testing.T) {
	if strategyBlockerTaskInScope(strategyBlockerTask("implementation")) {
		t.Fatal("implementation tasks must not be suppressed by strategy blocker hold")
	}
	if !strategyBlockerTaskInScope(strategyBlockerTask("strategy")) {
		t.Fatal("strategy project task should be in blocker hold scope")
	}
}

func TestStrategyBlockerTriggerBypassKeepsInternalRequestResumeDampened(t *testing.T) {
	if strategyBlockerTriggerBypasses(pendingWorkTrigger{Trigger: "request_resume"}) {
		t.Fatal("internal request_resume should still be dampened when coordination is unchanged")
	}
	if !strategyBlockerTriggerBypasses(pendingWorkTrigger{Trigger: "runtime_resume"}) {
		t.Fatal("explicit runtime_resume should bypass strategy blocker hold")
	}
	if !strategyBlockerTriggerBypasses(pendingWorkTrigger{Trigger: "inbound_message"}) {
		t.Fatal("inbound messages should bypass strategy blocker hold")
	}
}

func TestStrategyBlockerResultFingerprintNormalizesWhitespace(t *testing.T) {
	first := strategyBlockerResultFingerprint(strategyBlockerResult())
	secondResult := strategyBlockerResult()
	secondResult.Summary = "  Integration   blocked\nwaiting for branches "
	second := strategyBlockerResultFingerprint(secondResult)
	if first == "" || second == "" || first != second {
		t.Fatalf("expected whitespace-normalized fingerprints to match, first=%q second=%q", first, second)
	}
}

func strategyBlockerTask(lane string) WorkspaceTaskRecord {
	claimAgent := "agent-alpha"
	claimStatus := "CLAIMED"
	return WorkspaceTaskRecord{
		TaskID:       "root-task",
		Title:        "Sub-pixel art web app",
		Status:       "RUNNING",
		Priority:     "HIGH",
		TaskKind:     "PROJECT_ROOT",
		TaskTemplate: "project_root",
		ProjectID:    "project-subpixel",
		ProjectLane:  lane,
		ClaimAgentID: &claimAgent,
		ClaimStatus:  &claimStatus,
	}
}

func strategyBlockerResult() StructuredTaskResult {
	return StructuredTaskResult{
		Outcome: "blocked",
		Summary: "Integration blocked waiting for branches",
		Details: "Ready for review only exists for one lane.",
		BlockedOn: []BlockedRef{{
			Kind:   "dependency",
			Detail: "missing READY_FOR_REVIEW branch evidence",
		}},
	}
}

func strategyBlockerCoordinationRecord(headSHA, queueState string) ProjectCoordinationRecord {
	queueState = strings.TrimSpace(queueState)
	return ProjectCoordinationRecord{
		Project: ProjectRecord{
			ProjectID: "project-subpixel",
			Status:    "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			ProjectID:    "project-subpixel",
			CurrentPhase: "IMPLEMENTATION",
			RepoStatus:   "READY",
		},
		GateStatus: ProjectGateStatusRecord{
			ProjectID:           "project-subpixel",
			CurrentPhase:        "IMPLEMENTATION",
			OverallState:        "PARTIAL",
			ImplementationReady: false,
		},
		Branches: []ProjectBranchRecord{{
			BranchID:       "branch-beta",
			ProjectID:      "project-subpixel",
			RepoID:         "repo-1",
			AgentID:        "beta",
			ActiveTaskID:   "impl-beta",
			BranchName:     "agent/beta/subpixel",
			HeadSHA:        headSHA,
			BaseSHA:        "base123",
			ReviewDocKey:   "project.project-subpixel.branch.branch-beta.review",
			Status:         "READY_FOR_REVIEW",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			UpdatedAt:      "2026-05-08T02:00:00Z",
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "patchq-project-subpixel-repo-1",
			ItemID:          "patchitem-branch-beta",
			ProjectID:       "project-subpixel",
			RepoID:          "repo-1",
			BranchID:        "branch-beta",
			State:           queueState,
			HeadSHA:         headSHA,
			DecisionSummary: "same decision",
			UpdatedAt:       "2026-05-08T02:00:00Z",
		}},
		Tasks: []WorkspaceTaskRecord{
			strategyBlockerTask("strategy"),
			{
				TaskID:      "impl-beta",
				Status:      "RUNNING",
				ProjectID:   "project-subpixel",
				ProjectLane: "implementation",
				UpdatedAt:   "2026-05-08T02:00:00Z",
			},
		},
	}
}
