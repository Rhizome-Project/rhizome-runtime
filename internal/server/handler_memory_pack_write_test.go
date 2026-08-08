package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPackWriteRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-pack-write"
		agentID     = "agent-handler-memory-pack-write"
		sessionID   = "sess-handler-memory-pack-write"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Pack Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Pack Writer",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	summaryMemory, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "SUMMARY",
		Title:       "Handler compaction summary",
		Body:        "Canonical summary memory for handler pack write.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "compaction",
		SourceID:    sessionID,
	})
	if err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPackWriteParams{
		WorkspaceID:            workspaceID,
		SessionID:              sessionID,
		PackMode:               "DETERMINISTIC_FALLBACK",
		SourceWindowDigest:     "digest-handler-memory-pack-write",
		TokenBudget:            2048,
		MessageCountBefore:     10,
		MessageCountAfter:      4,
		MessageTokensBefore:    2200,
		MessageTokensAfter:     900,
		TotalInputTokens:       3600,
		TotalOutputTokens:      1200,
		SummaryText:            "[Previous conversation history was truncated due to length. 6 messages were removed.]",
		SummaryWorkspaceMemory: summaryMemory.MemoryID,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPackWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPackWrite rpc error: %+v", rpcErr)
	}
	writeResult, ok := result.(sqlite.MemoryPackWriteResult)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}
	if writeResult.Snapshot.AgentID != agentID || writeResult.Pack.PackID != writeResult.Snapshot.EpisodePackID {
		t.Fatalf("unexpected memory pack write payload %+v", writeResult)
	}

	listRaw, err := json.Marshal(workspaceMemoryPackListParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listResult, rpcErr := h.workspaceMemoryPackList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPackList rpc error after write: %+v", rpcErr)
	}
	listPayload := listResult.(map[string]any)
	items, ok := listPayload["items"].([]sqlite.EpisodePackRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", listPayload["items"])
	}
	if len(items) != 1 || items[0].PackID != writeResult.Pack.PackID {
		t.Fatalf("unexpected memory pack list payload after write %+v", listPayload)
	}

	getRaw, err := json.Marshal(workspaceMemoryPackGetParams{
		WorkspaceID: workspaceID,
		PackID:      writeResult.Pack.PackID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getResult, rpcErr := h.workspaceMemoryPackGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPackGet rpc error after write: %+v", rpcErr)
	}
	getPayload := getResult.(map[string]any)
	record, ok := getPayload["pack"].(sqlite.EpisodePackRecord)
	if !ok {
		t.Fatalf("unexpected get pack type %T", getPayload["pack"])
	}
	if record.PackID != writeResult.Pack.PackID || record.CompactionSnapshotID != writeResult.Snapshot.SnapshotID {
		t.Fatalf("unexpected memory pack get payload after write %+v", getPayload)
	}
}

func TestWorkspaceMemoryPackWriteRejectsAuthorityFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw := []byte(`{
		"workspace_id":"ws-handler-memory-pack-authority",
		"session_id":"sess-1",
		"pack_id":"pack-1",
		"pack_type":"SESSION_BLOCKED",
		"decision_ledger":["should fail"]
	}`)
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "direct pack authority fields") {
		t.Fatalf("expected pack authority rejection, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryPackWriteRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	raw := []byte(`{
		"workspace_id":"ws-handler-memory-pack-unknown",
		"session_id":"sess-1",
		"summary_digest":"should-fail"
	}`)
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "unknown fields") {
		t.Fatalf("expected unknown field rejection, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryPackWriteRejectsMissingSummaryWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-pack-summary"
		agentID     = "agent-handler-memory-pack-summary"
		sessionID   = "sess-handler-memory-pack-summary"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Pack Summary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Pack Summary Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	raw := []byte(`{
		"workspace_id":"ws-handler-memory-pack-summary",
		"session_id":"sess-handler-memory-pack-summary",
		"summary_workspace_memory":"mem-missing"
	}`)
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "summary_workspace_memory") {
		t.Fatalf("expected summary workspace memory rejection, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryPackWriteReusesDuplicateSnapshotAndValidationErrorsAsInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-pack-write-invalid"
		agentID     = "agent-handler-memory-pack-write-invalid"
		agentOther  = "agent-handler-memory-pack-write-other"
		sessionID   = "sess-handler-memory-pack-write-invalid"
		snapshotID  = "snapshot-handler-memory-pack-write-invalid"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Pack Write Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, id := range []string{agentID, agentOther} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     id,
			OwnerUserID: "developer",
			DisplayName: id,
		}); err != nil {
			t.Fatalf("register agent %s: %v", id, err)
		}
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	firstRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		SnapshotID:  snapshotID,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
	})
	if err != nil {
		t.Fatalf("marshal first write params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, firstRaw); rpcErr != nil {
		t.Fatalf("first workspaceMemoryPackWrite rpc error: %+v", rpcErr)
	}

	duplicateRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		SnapshotID:  snapshotID,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
	})
	if err != nil {
		t.Fatalf("marshal duplicate params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPackWrite(ctx, duplicateRaw)
	if rpcErr != nil {
		t.Fatalf("expected duplicate snapshot reuse, got %+v", rpcErr)
	}
	writeResult, ok := result.(sqlite.MemoryPackWriteResult)
	if !ok {
		t.Fatalf("unexpected duplicate result type %T", result)
	}
	if writeResult.Snapshot.SnapshotID != snapshotID || writeResult.Pack.CompactionSnapshotID != snapshotID {
		t.Fatalf("expected duplicate snapshot reuse to return canonical projection, got %+v", writeResult)
	}

	replayMismatchRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		SnapshotID:         snapshotID,
		WorkspaceID:        workspaceID,
		SessionID:          sessionID,
		PackMode:           "DETERMINISTIC_FALLBACK",
		SourceWindowDigest: "digest-mismatch",
	})
	if err != nil {
		t.Fatalf("marshal replay mismatch params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, replayMismatchRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "snapshot_id replay payload does not match existing snapshot") {
		t.Fatalf("expected invalid params for replay mismatch, got %+v", rpcErr)
	}

	mismatchRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentOther,
	})
	if err != nil {
		t.Fatalf("marshal mismatch params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, mismatchRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "does not belong to agent_id") {
		t.Fatalf("expected invalid params for session/agent mismatch, got %+v", rpcErr)
	}

	packModeRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		PackMode:    "NOT_A_MODE",
	})
	if err != nil {
		t.Fatalf("marshal invalid pack mode params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, packModeRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for invalid pack_mode, got %+v", rpcErr)
	}

	triggerKindRaw, err := json.Marshal(workspaceMemoryPackWriteParams{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		TriggerKind: "policy_override",
	})
	if err != nil {
		t.Fatalf("marshal invalid trigger kind params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPackWrite(ctx, triggerKindRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "trigger_kind") {
		t.Fatalf("expected invalid params for invalid trigger_kind, got %+v", rpcErr)
	}
}
