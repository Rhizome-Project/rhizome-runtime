package living_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryWriteTool_SurfacesPromotedClaimEffects(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-living-memory-write-effects"
		agentID     = "agent-living-memory-write-effects"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Workspace Memory Write Effects",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Memory Write Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryWriteTool(client, workspaceID, agentID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"type":"decision",
		"topic":"Living promoted claim output",
		"content":"Tool memory_write should surface promoted claim effects for promotable memory.",
		"summary":"Living tool promoted claim."
	}`))
	if err != nil {
		t.Fatalf("memory_write execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_write output: %v", err)
	}
	if payload["status"] != "saved" {
		t.Fatalf("expected saved status, got %+v", payload)
	}
	memoryMap, ok := payload["memory"].(map[string]any)
	if !ok || memoryMap["memory_id"] == nil {
		t.Fatalf("expected memory payload with memory_id, got %+v", payload)
	}
	memoryID, ok := memoryMap["memory_id"].(string)
	if !ok || memoryID == "" {
		t.Fatalf("expected string memory_id in output, got %+v", memoryMap)
	}
	effects, ok := payload["promoted_claim_effects"].(map[string]any)
	if !ok {
		t.Fatalf("expected promoted_claim_effects in output, got %+v", payload)
	}
	claimMap, ok := effects["claim"].(map[string]any)
	if !ok {
		t.Fatalf("expected claim in promoted_claim_effects, got %+v", effects)
	}
	claimEventMap, ok := effects["claim_event"].(map[string]any)
	if !ok {
		t.Fatalf("expected claim_event in promoted_claim_effects, got %+v", effects)
	}
	wantClaimID := "claim:memory:" + memoryID
	if claimMap["claim_id"] != wantClaimID {
		t.Fatalf("expected surfaced promoted claim %q, got %+v", wantClaimID, claimMap)
	}
	if claimEventMap["event_type"] != "knowledge_claim.written" || claimEventMap["entity_id"] != wantClaimID {
		t.Fatalf("expected surfaced promoted claim event for %q, got %+v", wantClaimID, claimEventMap)
	}
	if _, ok := effects["queue"]; ok {
		t.Fatalf("did not expect queue effect on direct memory_write output, got %+v", effects)
	}
	if _, ok := effects["queue_event"]; ok {
		t.Fatalf("did not expect queue_event effect on direct memory_write output, got %+v", effects)
	}
	if _, ok := effects["invalidation_events"]; ok {
		t.Fatalf("did not expect invalidation_events on direct memory_write output, got %+v", effects)
	}
}

func TestWorkspaceMemoryToolsAdvertiseAndPersistAntiProcedure(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-living-anti-procedure"
		agentID     = "agent-living-anti-procedure"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Anti Procedure Memory",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Anti Procedure Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	readTool := living.NewWorkspaceMemoryReadTool(client, workspaceID)
	writeTool := living.NewWorkspaceMemoryWriteTool(client, workspaceID, agentID)

	readSchema := readTool.Schema()
	readTypeProp, ok := readSchema.Properties["type"]
	if !ok {
		t.Fatalf("memory_read schema missing type property: %+v", readSchema)
	}
	for _, want := range []string{"ANTI_PROCEDURE", "EXPERIENCE", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE"} {
		foundRead := false
		for _, item := range readTypeProp.Enum {
			if item == want {
				foundRead = true
				break
			}
		}
		if !foundRead {
			t.Fatalf("memory_read schema missing %s enum: %+v", want, readTypeProp.Enum)
		}
	}

	writeSchema := writeTool.Schema()
	writeTypeProp, ok := writeSchema.Properties["type"]
	if !ok {
		t.Fatalf("memory_write schema missing type property: %+v", writeSchema)
	}
	for _, want := range []string{"ANTI_PROCEDURE", "EXPERIENCE", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE"} {
		foundWrite := false
		for _, item := range writeTypeProp.Enum {
			if item == want {
				foundWrite = true
				break
			}
		}
		if !foundWrite {
			t.Fatalf("memory_write schema missing %s enum: %+v", want, writeTypeProp.Enum)
		}
	}

	out, err := writeTool.Execute(ctx, json.RawMessage(`{
		"type":"anti_procedure",
		"topic":"Rollback bypass stays forbidden",
		"content":"Do not bypass live doctor or rollback-gate checks during degraded telemetry.",
		"summary":"Anti-procedure living write."
	}`))
	if err != nil {
		t.Fatalf("memory_write anti procedure execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode anti procedure memory_write output: %v", err)
	}
	effects, ok := payload["promoted_claim_effects"].(map[string]any)
	if !ok {
		t.Fatalf("expected promoted_claim_effects in output, got %+v", payload)
	}
	claimMap, ok := effects["claim"].(map[string]any)
	if !ok {
		t.Fatalf("expected claim in promoted_claim_effects, got %+v", effects)
	}
	if claimMap["claim_type"] != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure promoted claim type, got %+v", claimMap)
	}

	readOut, err := readTool.Execute(ctx, json.RawMessage(`{"type":"ANTI_PROCEDURE","limit":10}`))
	if err != nil {
		t.Fatalf("memory_read anti procedure execute failed: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(readOut), &items); err != nil {
		t.Fatalf("decode anti procedure memory_read output: %v", err)
	}
	if len(items) != 1 || items[0]["memory_type"] != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure memory_read result, got %+v", items)
	}
}

func TestWorkspaceMemoryToolsAdvertiseAndPersistIdentityGovernanceTypes(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-living-identity-governance"
		agentID     = "agent-living-identity-governance"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Identity Governance Memory",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Identity Governance Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	readTool := living.NewWorkspaceMemoryReadTool(client, workspaceID)
	writeTool := living.NewWorkspaceMemoryWriteTool(client, workspaceID, agentID)

	out, err := writeTool.Execute(ctx, json.RawMessage(`{
		"type":"self_model",
		"topic":"Current operating stance",
		"content":"Identity/governance writes should preserve already-supported self-model memory types on the direct tool path.",
		"summary":"Self-model living write."
	}`))
	if err != nil {
		t.Fatalf("memory_write self model execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode self model memory_write output: %v", err)
	}
	memoryMap, ok := payload["memory"].(map[string]any)
	if !ok || memoryMap["memory_type"] != "SELF_MODEL" {
		t.Fatalf("expected self model memory payload, got %+v", payload)
	}
	if _, ok := payload["promoted_claim_effects"]; ok {
		t.Fatalf("did not expect self model tool write to surface promoted claim effects, got %+v", payload)
	}

	readOut, err := readTool.Execute(ctx, json.RawMessage(`{"type":"SELF_MODEL","limit":10}`))
	if err != nil {
		t.Fatalf("memory_read self model execute failed: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(readOut), &items); err != nil {
		t.Fatalf("decode self model memory_read output: %v", err)
	}
	if len(items) != 1 || items[0]["memory_type"] != "SELF_MODEL" {
		t.Fatalf("expected self model memory_read result, got %+v", items)
	}
}

func TestWorkspaceMemoryToolsAdvertiseAndPersistGoalCommitmentAndPolicyTrace(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-identity-governance-tightener"
		agentID     = "agent-living-identity-governance-tightener"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Goal Commitment and Policy Trace Memory",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Goal Commitment and Policy Trace Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	readTool := living.NewWorkspaceMemoryReadTool(client, workspaceID)
	writeTool := living.NewWorkspaceMemoryWriteTool(client, workspaceID, agentID)

	for _, tc := range []struct {
		writeType string
		wantType  string
		topic     string
		content   string
		summary   string
	}{
		{
			writeType: "goal_commitment",
			wantType:  "GOAL_COMMITMENT",
			topic:     "Protect the contour",
			content:   "Living memory writes should preserve goal-commitment identity memory types on the direct tool path.",
			summary:   "Goal-commitment living write.",
		},
		{
			writeType: "policy_trace",
			wantType:  "POLICY_TRACE",
			topic:     "Escalation policy trace",
			content:   "Living memory writes should preserve policy-trace identity memory types on the direct tool path.",
			summary:   "Policy-trace living write.",
		},
	} {
		out, err := writeTool.Execute(ctx, json.RawMessage(`{
			"type":"`+tc.writeType+`",
			"topic":"`+tc.topic+`",
			"content":"`+tc.content+`",
			"summary":"`+tc.summary+`"
		}`))
		if err != nil {
			t.Fatalf("memory_write %s execute failed: %v", tc.writeType, err)
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("decode %s memory_write output: %v", tc.writeType, err)
		}
		memoryMap, ok := payload["memory"].(map[string]any)
		if !ok || memoryMap["memory_type"] != tc.wantType {
			t.Fatalf("expected %s memory payload, got %+v", tc.wantType, payload)
		}
		if _, ok := payload["promoted_claim_effects"]; ok {
			t.Fatalf("did not expect %s tool write to surface promoted claim effects, got %+v", tc.writeType, payload)
		}

		readOut, err := readTool.Execute(ctx, json.RawMessage(`{"type":"`+tc.wantType+`","limit":10}`))
		if err != nil {
			t.Fatalf("memory_read %s execute failed: %v", tc.writeType, err)
		}
		var items []map[string]any
		if err := json.Unmarshal([]byte(readOut), &items); err != nil {
			t.Fatalf("decode %s memory_read output: %v", tc.writeType, err)
		}
		if len(items) != 1 || items[0]["memory_type"] != tc.wantType {
			t.Fatalf("expected %s memory_read result, got %+v", tc.wantType, items)
		}
	}
}

func TestWorkspaceMemoryWriteToolRejectsUnsupportedCurrentDirectType(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-invalid-direct-type"
		agentID     = "agent-living-invalid-direct-type"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Invalid Direct Type",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Invalid Direct Type Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	writeTool := living.NewWorkspaceMemoryWriteTool(client, workspaceID, agentID)

	_, err := writeTool.Execute(ctx, json.RawMessage(`{
		"type":"constitution",
		"content":"This tool boundary should reject unsupported current direct memory types."
	}`))
	if err == nil || !strings.Contains(err.Error(), "type must be one of") {
		t.Fatalf("expected unsupported type rejection, got %v", err)
	}
}

func TestWorkspaceMemoryPacketShellToolReturnsShellPacket(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-packet-shell"
		agentID     = "agent-living-memory-packet-shell"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-tool-shell-self",
		MemoryType:  "SELF_MODEL",
		Title:       "Tool self model",
		Body:        "Living AMS shell packet facade should surface task-scoped identity memories.",
		Summary:     "Tool shell self model.",
		SourceKind:  "manual",
		SourceID:    agentID,
		TaskID:      taskID,
		AgentID:     agentID,
		Importance:  0.91,
	}); err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryPacketShellTool(client, workspaceID, agentID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"task_id":"`+taskID+`"
	}`))
	if err != nil {
		t.Fatalf("memory_packet_shell execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_packet_shell output: %v", err)
	}
	packetMap, ok := payload["packet"].(map[string]any)
	if !ok {
		t.Fatalf("expected packet payload, got %+v", payload)
	}
	metaMap, ok := packetMap["meta"].(map[string]any)
	if !ok || metaMap["packet_kind"] != "SHELL" || metaMap["task_id"] != taskID {
		t.Fatalf("expected shell packet meta, got %+v", payload)
	}
	kernelRef, ok := packetMap["kernel_ref"].(map[string]any)
	if !ok || kernelRef["packet_key"] == "" || kernelRef["basis_digest"] == "" {
		t.Fatalf("expected shell packet to preserve kernel_ref, got %+v", packetMap)
	}
	boundarySummary, ok := payload["boundary_summary"].(map[string]any)
	if !ok || boundarySummary["identity_memory_count"] == nil || boundarySummary["trace_context_count"] == nil {
		t.Fatalf("expected top-level boundary summary mirror in shell tool output, got %+v", payload)
	}
	basisSummary, ok := payload["basis_summary"].(map[string]any)
	if !ok || basisSummary["total_ref_count"] == nil || basisSummary["recent_trace_basis_count"] == nil {
		t.Fatalf("expected top-level basis summary mirror in shell tool output, got %+v", payload)
	}
	identityMemories, ok := packetMap["identity_memories"].([]any)
	if !ok || len(identityMemories) == 0 {
		t.Fatalf("expected identity memories in shell packet, got %+v", packetMap)
	}
}

func TestWorkspaceMemoryPacketKernelToolReturnsKernelPacket(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-packet-kernel"
		agentID     = "agent-living-memory-packet-kernel"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if _, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryID:    "mem-tool-kernel-constraint",
		MemoryType:  "SUMMARY",
		Title:       "Kernel packet memory",
		Body:        "Kernel packet facade should surface bounded packet summaries.",
		Summary:     "Kernel packet summary.",
		SourceKind:  "manual",
		SourceID:    agentID,
		TaskID:      taskID,
		AgentID:     agentID,
		Importance:  0.77,
	}); err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryPacketKernelTool(client, workspaceID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"task_id":"`+taskID+`"
	}`))
	if err != nil {
		t.Fatalf("memory_packet_kernel execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_packet_kernel output: %v", err)
	}
	packetMap, ok := payload["packet"].(map[string]any)
	if !ok {
		t.Fatalf("expected packet payload, got %+v", payload)
	}
	metaMap, ok := packetMap["meta"].(map[string]any)
	if !ok || metaMap["packet_kind"] != "KERNEL" || metaMap["task_id"] != taskID {
		t.Fatalf("expected kernel packet meta, got %+v", payload)
	}
	boundarySummary, ok := payload["boundary_summary"].(map[string]any)
	if !ok || boundarySummary["decision_record_count"] == nil || boundarySummary["active_blocker_count"] == nil {
		t.Fatalf("expected top-level boundary summary mirror in kernel tool output, got %+v", payload)
	}
	basisSummary, ok := payload["basis_summary"].(map[string]any)
	if !ok || basisSummary["total_ref_count"] == nil || basisSummary["coordination_basis_count"] == nil {
		t.Fatalf("expected top-level basis summary mirror in kernel tool output, got %+v", payload)
	}
}

func TestWorkspaceMemoryPacketShellToolRequiresTaskOrSession(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-packet-shell-invalid"
		agentID     = "agent-living-memory-packet-shell-invalid"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Shell Packet Invalid",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Shell Packet Invalid Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryPacketShellTool(client, workspaceID, agentID)

	_, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "task_id or session_id is required") {
		t.Fatalf("expected missing task/session rejection, got %v", err)
	}
}

func TestWorkspaceMemoryPacketKernelToolRequiresTaskOrSession(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-living-memory-packet-kernel-invalid"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Kernel Packet Invalid",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	tool := living.NewWorkspaceMemoryPacketKernelTool(client, workspaceID)

	_, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "task_id or session_id is required") {
		t.Fatalf("expected missing task/session rejection, got %v", err)
	}
}

func TestWorkspaceMemoryPromotionReadToolListsCandidates(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-promotion-read"
		agentID     = "agent-living-memory-promotion-read"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")
	sessionID := "sess-living-memory-promotion-read"

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-living-promotion-read",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.25,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:living-promotion-read",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-living-promotion-read", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "lesson",
			Title:      "Promotion read candidate",
			Body:       "Living AMS promotion facade should stay read-only while surfacing advisory review state.",
			TaskID:     taskID,
			AgentID:    agentID,
			SessionID:  sessionID,
			SourceKind: "memory_packet_shell",
			SourceID:   "shell-packet-tool-promotion",
		},
		BasisDigest: "basis-digest-tool-promotion",
		BasisRefs:   []string{"packet:shell-packet-tool-promotion"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryPromotionReadTool(client, workspaceID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"state":"PENDING",
		"limit":10
	}`))
	if err != nil {
		t.Fatalf("memory_promotion_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_promotion_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace_id %q, got %+v", workspaceID, payload)
	}
	if payload["count"] != float64(1) {
		t.Fatalf("expected count=1, got %+v", payload)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one advisory promotion item, got %+v", payload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["promotion_id"] != record.PromotionID || item["state"] != "PENDING" {
		t.Fatalf("expected pending promotion payload, got %+v", items[0])
	}
	if item["candidate_type"] != "LESSON" {
		t.Fatalf("expected candidate_type LESSON, got %+v", item)
	}
	advisoryItems, ok := payload["advisory_items"].([]any)
	if !ok || len(advisoryItems) != 1 {
		t.Fatalf("expected one advisory promotion item, got %+v", payload)
	}
	advisory, ok := advisoryItems[0].(map[string]any)
	if !ok || advisory["promotion_id"] != record.PromotionID || advisory["review_action"] != "DEFER_ACCEPT" || advisory["source"] != "promotion_record" {
		t.Fatalf("expected degraded advisory review action from promotion record, got %+v", advisoryItems[0])
	}
	if advisory["coherence_band"] != "DEGRADED" || advisory["needs_attention"] != true {
		t.Fatalf("expected degraded advisory coherence fields, got %+v", advisory)
	}
}

func TestWorkspaceMemoryPromotionReadToolGetsOneCandidate(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-promotion-get"
		agentID     = "agent-living-memory-promotion-get"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.EnqueueMemoryPromotion(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "decision",
			Title:      "Promotion get candidate",
			Body:       "Living AMS promotion facade should be able to fetch one advisory candidate by id.",
			TaskID:     taskID,
			AgentID:    agentID,
			SourceKind: "memory_packet_shell",
			SourceID:   "shell-packet-tool-promotion-get",
		},
		BasisDigest: "basis-digest-tool-promotion-get",
		BasisRefs:   []string{"packet:shell-packet-tool-promotion-get"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryPromotionReadTool(client, workspaceID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"promotion_id":"`+record.PromotionID+`"
	}`))
	if err != nil {
		t.Fatalf("memory_promotion_read single execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode single memory_promotion_read output: %v", err)
	}
	promotion, ok := payload["promotion"].(map[string]any)
	if !ok || promotion["promotion_id"] != record.PromotionID || promotion["candidate_type"] != "DECISION" {
		t.Fatalf("expected single promotion payload, got %+v", payload)
	}
	advisory, ok := payload["advisory"].(map[string]any)
	if !ok || advisory["promotion_id"] != record.PromotionID || advisory["review_action"] != "REVIEW" || advisory["source"] != "promotion_record" {
		t.Fatalf("expected default advisory review payload, got %+v", payload)
	}
	if advisory["needs_attention"] != false {
		t.Fatalf("expected default advisory payload without attention, got %+v", advisory)
	}
}

func TestWorkspaceMemoryCoherenceReadToolReturnsScope(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-coherence-read"
		agentID     = "agent-living-memory-coherence-read"
		sessionID   = "sess-living-memory-coherence-read"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-living-coherence-read",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.25,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:living-coherence-read",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-living-coherence-read", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryCoherenceReadTool(client, workspaceID)

	out, err := tool.Execute(ctx, json.RawMessage(`{
		"agent_id":"`+agentID+`",
		"session_id":"`+sessionID+`",
		"report_scope":"SESSION"
	}`))
	if err != nil {
		t.Fatalf("memory_coherence_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_coherence_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace_id %q, got %+v", workspaceID, payload)
	}
	scope, ok := payload["scope"].(map[string]any)
	if !ok {
		t.Fatalf("expected scope payload, got %+v", payload)
	}
	if scope["agent_id"] != agentID || scope["session_id"] != sessionID || scope["report_scope"] != "SESSION" {
		t.Fatalf("expected scoped coherence payload, got %+v", scope)
	}
	if scope["coherence_band_hint"] != "DEGRADED" || scope["needs_attention"] != true {
		t.Fatalf("expected degraded coherence scope, got %+v", scope)
	}
	if scope["ready_invalidation_count"] != float64(1) {
		t.Fatalf("expected ready invalidation count to stay surfaced, got %+v", scope)
	}
}

func TestWorkspaceMemoryCoherenceReadToolRequiresAgentID(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-living-memory-coherence-read-invalid"
		agentID     = "agent-living-memory-coherence-read-invalid"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Living Coherence Read Invalid",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Living Coherence Read Invalid Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	tool := living.NewWorkspaceMemoryCoherenceReadTool(client, workspaceID)

	_, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "agent_id is required") {
		t.Fatalf("expected missing agent_id rejection, got %v", err)
	}
}

func TestWorkspaceMemoryInvalidationReadToolsReturnBoundedData(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-tool"
		agentID     = "agent-memory-invalidation-tool"
		sessionID   = "sess-memory-invalidation-tool"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   "2026-03-29T00:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-tool-invalidation",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.25,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:tool-invalidation",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-tool-invalidation", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	polled, _, err := store.PollMemoryInvalidationsWithEvents(ctx, sqlite.MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("expected one invalidation after poll, got %+v", polled)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	listTool := living.NewWorkspaceMemoryInvalidationReadTool(client, workspaceID, agentID)
	itemTool := living.NewWorkspaceMemoryInvalidationItemReadTool(client, workspaceID, agentID)
	cursorTool := living.NewWorkspaceMemoryInvalidationCursorReadTool(client, workspaceID, agentID)

	listOut, err := listTool.Execute(ctx, json.RawMessage(`{"session_id":"`+sessionID+`"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_read execute failed: %v", err)
	}
	var listPayload map[string]any
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode memory_invalidation_read output: %v", err)
	}
	if listPayload["workspace_id"] != workspaceID || listPayload["agent_id"] != agentID || listPayload["count"] != float64(1) {
		t.Fatalf("expected workspace/agent/count mirrors, got %+v", listPayload)
	}
	if _, ok := listPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", listPayload)
	}
	listItems, ok := listPayload["items"].([]any)
	if !ok || len(listItems) != 1 {
		t.Fatalf("expected one bounded invalidation item, got %+v", listPayload)
	}
	listItem, ok := listItems[0].(map[string]any)
	if !ok || listItem["invalidation_id"] != polled[0].InvalidationID || listItem["delivered_at"] == "" {
		t.Fatalf("expected delivered invalidation payload, got %+v", listPayload)
	}

	itemOut, err := itemTool.Execute(ctx, json.RawMessage(`{"invalidation_id":"`+polled[0].InvalidationID+`"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_item_read execute failed: %v", err)
	}
	var itemPayload map[string]any
	if err := json.Unmarshal([]byte(itemOut), &itemPayload); err != nil {
		t.Fatalf("decode memory_invalidation_item_read output: %v", err)
	}
	invalidation, ok := itemPayload["invalidation"].(map[string]any)
	if !ok || invalidation["invalidation_id"] != polled[0].InvalidationID || invalidation["reason"] != polled[0].Reason {
		t.Fatalf("expected bounded invalidation item payload, got %+v", itemPayload)
	}

	cursorOut, err := cursorTool.Execute(ctx, json.RawMessage(`{"session_id":"`+sessionID+`"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_cursor_read execute failed: %v", err)
	}
	var cursorPayload map[string]any
	if err := json.Unmarshal([]byte(cursorOut), &cursorPayload); err != nil {
		t.Fatalf("decode memory_invalidation_cursor_read output: %v", err)
	}
	cursor, ok := cursorPayload["cursor"].(map[string]any)
	if !ok || cursor["last_delivered_invalidation_id"] != polled[0].InvalidationID || cursor["last_poll_count"] != float64(1) {
		t.Fatalf("expected bounded invalidation cursor payload, got %+v", cursorPayload)
	}
}

func TestWorkspaceMemoryGraphReadToolsReturnBoundedData(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-tool"
		agentID     = "agent-memory-graph-tool"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Memory graph tool lesson",
		Body:        "Graph facades should stay read-only on the living boundary.",
		Summary:     "bounded graph detail",
		AgentID:     agentID,
		TaskID:      taskID,
		SourceKind:  "workspace_memory_write",
		SourceID:    "tool-test",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)

	listTool := living.NewWorkspaceMemoryGraphListReadTool(client, workspaceID, agentID)
	getTool := living.NewWorkspaceMemoryGraphGetReadTool(client, workspaceID)

	listOut, err := listTool.Execute(ctx, json.RawMessage(`{"task_id":"`+taskID+`","origin_kind":"workspace_memory"}`))
	if err != nil {
		t.Fatalf("memory_graph_list_read execute failed: %v", err)
	}
	var listPayload map[string]any
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode memory_graph_list_read output: %v", err)
	}
	if listPayload["workspace_id"] != workspaceID || listPayload["count"] != float64(1) {
		t.Fatalf("expected workspace/count mirrors, got %+v", listPayload)
	}
	if _, ok := listPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", listPayload)
	}
	items, ok := listPayload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one bounded graph item, got %+v", listPayload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["memory_id"] != nodeID || item["task_id"] != taskID {
		t.Fatalf("expected canonical graph node payload, got %+v", listPayload)
	}

	getOut, err := getTool.Execute(ctx, json.RawMessage(`{"memory_id":"`+nodeID+`"}`))
	if err != nil {
		t.Fatalf("memory_graph_get_read execute failed: %v", err)
	}
	var getPayload map[string]any
	if err := json.Unmarshal([]byte(getOut), &getPayload); err != nil {
		t.Fatalf("decode memory_graph_get_read output: %v", err)
	}
	if getPayload["workspace_id"] != workspaceID || getPayload["memory_id"] != nodeID {
		t.Fatalf("expected workspace/memory mirrors, got %+v", getPayload)
	}
	if _, ok := getPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", getPayload)
	}
	node, ok := getPayload["node"].(map[string]any)
	if !ok || node["memory_id"] != nodeID || node["semantic_lineage_id"] != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected bounded graph node detail, got %+v", getPayload)
	}
	if _, ok := getPayload["detail"].(map[string]any); !ok {
		t.Fatalf("expected canonical detail payload, got %+v", getPayload)
	}
}

func TestWorkspaceMemoryNodeSearchReadToolReturnsBoundedData(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-node-search-tool"
		agentID     = "agent-memory-node-search-tool"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "code_review")

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Memory node search tool lesson",
		Body:        "Canonical node search should stay read-only on the living boundary.",
		Summary:     "bounded node search detail",
		AgentID:     agentID,
		TaskID:      taskID,
		SourceKind:  "workspace_memory_write",
		SourceID:    "tool-search-test",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID); err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceMemoryNodeSearchReadTool(client, workspaceID, agentID)

	out, err := tool.Execute(ctx, json.RawMessage(`{"query":"read-only on the living boundary","origin_kind":"workspace_memory","task_id":"`+taskID+`"}`))
	if err != nil {
		t.Fatalf("memory_node_search_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode memory_node_search_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID || payload["query"] != "read-only on the living boundary" || payload["count"] != float64(1) {
		t.Fatalf("expected bounded node-search mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if generatedAt, ok := payload["generated_at"].(string); !ok || strings.TrimSpace(generatedAt) == "" {
		t.Fatalf("expected generated_at mirror, got %+v", payload)
	}
	hits, ok := payload["hits"].([]any)
	if !ok || len(hits) != 1 {
		t.Fatalf("expected one bounded node-search hit, got %+v", payload)
	}
	hit, ok := hits[0].(map[string]any)
	if !ok || hit["memory_id"] != nodeID || hit["snippet"] == "" {
		t.Fatalf("expected canonical node-search hit payload, got %+v", payload)
	}
	if _, ok := payload["result"].(map[string]any); !ok {
		t.Fatalf("expected canonical result payload, got %+v", payload)
	}
}

func TestWorkspaceTensionReadToolsReturnBoundedData(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "living-tension-tools")
	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount+refresh.UpdatedCount+refresh.RecoveredCount == 0 {
		t.Fatalf("expected refresh to touch canonical tensions, got %+v", refresh)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	client.SetAgentID("agent-a")
	listTool := living.NewWorkspaceTensionListReadTool(client, scenario.workspaceID, "agent-a")
	getTool := living.NewWorkspaceTensionGetReadTool(client, scenario.workspaceID)
	frontierTool := living.NewWorkspaceTensionFrontierReadTool(client, scenario.workspaceID, "agent-a")

	listOut, err := listTool.Execute(ctx, json.RawMessage(`{"task_id":"`+scenario.taskID+`"}`))
	if err != nil {
		t.Fatalf("tension_list_read execute failed: %v", err)
	}
	var listPayload map[string]any
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode tension_list_read output: %v", err)
	}
	if listPayload["workspace_id"] != scenario.workspaceID || listPayload["count"] == float64(0) {
		t.Fatalf("expected workspace/count mirrors, got %+v", listPayload)
	}
	if _, ok := listPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", listPayload)
	}
	listItems, ok := listPayload["items"].([]any)
	if !ok || len(listItems) == 0 {
		t.Fatalf("expected bounded tension list items, got %+v", listPayload)
	}
	listItem, ok := listItems[0].(map[string]any)
	if !ok || listItem["proto_cluster_id"] != scenario.protoClusterID || listItem["tension_id"] == "" {
		t.Fatalf("expected canonical tension list item, got %+v", listPayload)
	}
	primaryID, _ := listItem["tension_id"].(string)

	getOut, err := getTool.Execute(ctx, json.RawMessage(`{"tension_id":"`+primaryID+`"}`))
	if err != nil {
		t.Fatalf("tension_get_read execute failed: %v", err)
	}
	var getPayload map[string]any
	if err := json.Unmarshal([]byte(getOut), &getPayload); err != nil {
		t.Fatalf("decode tension_get_read output: %v", err)
	}
	if getPayload["workspace_id"] != scenario.workspaceID || getPayload["tension_id"] != primaryID {
		t.Fatalf("expected workspace/tension mirrors, got %+v", getPayload)
	}
	if _, ok := getPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", getPayload)
	}
	tension, ok := getPayload["tension"].(map[string]any)
	if !ok || tension["tension_id"] != primaryID || tension["proto_cluster_id"] != scenario.protoClusterID {
		t.Fatalf("expected canonical tension detail payload, got %+v", getPayload)
	}
	if _, ok := getPayload["detail"].(map[string]any); !ok {
		t.Fatalf("expected canonical detail envelope, got %+v", getPayload)
	}

	frontierOut, err := frontierTool.Execute(ctx, json.RawMessage(`{"task_id":"`+scenario.taskID+`"}`))
	if err != nil {
		t.Fatalf("tension_frontier_read execute failed: %v", err)
	}
	var frontierPayload map[string]any
	if err := json.Unmarshal([]byte(frontierOut), &frontierPayload); err != nil {
		t.Fatalf("decode tension_frontier_read output: %v", err)
	}
	if frontierPayload["workspace_id"] != scenario.workspaceID || frontierPayload["count"] == float64(0) {
		t.Fatalf("expected frontier workspace/count mirrors, got %+v", frontierPayload)
	}
	if _, ok := frontierPayload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", frontierPayload)
	}
	frontierItems, ok := frontierPayload["items"].([]any)
	if !ok || len(frontierItems) == 0 {
		t.Fatalf("expected bounded frontier items, got %+v", frontierPayload)
	}
	frontierItem, ok := frontierItems[0].(map[string]any)
	if !ok || frontierItem["proto_cluster_id"] != scenario.protoClusterID || frontierItem["tension_id"] == "" {
		t.Fatalf("expected canonical frontier item, got %+v", frontierPayload)
	}
}

func TestWorkspaceTensionAttachableReadToolReturnsBoundedData(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	scenario := seedBlockedControlClusterScenario(t, ctx, store, "living-tension-attachable-tool")
	if _, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
		Limit:       50,
	}); err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, scenario.workspaceID)
	client.SetAgentID("agent-a")
	tool := living.NewWorkspaceTensionAttachableReadTool(client, scenario.workspaceID, "agent-a")

	out, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("tension_attachable_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode tension_attachable_read output: %v", err)
	}
	if payload["workspace_id"] != scenario.workspaceID || payload["agent_id"] != "agent-a" || payload["count"] == float64(0) {
		t.Fatalf("expected workspace/agent/count mirrors, got %+v", payload)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected bounded attachable items, got %+v", payload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["tension_id"] == "" {
		t.Fatalf("expected canonical scored tension payload, got %+v", payload)
	}
	if attachProb, ok := item["attach_prob"].(float64); !ok || attachProb <= 0 {
		t.Fatalf("expected surfaced attach probability, got %+v", payload)
	}
	attachFactors, ok := item["attach_factors"].(map[string]any)
	if !ok || attachFactors["fit"] == nil {
		t.Fatalf("expected surfaced attach factors, got %+v", payload)
	}
	if attachFactors["novelty"] == nil && attachFactors["stay_bonus"] == nil && attachFactors["exploration_prior"] == nil {
		t.Fatalf("expected surfaced attach factors, got %+v", payload)
	}
}

func TestWorkspaceRSPStateReadToolReturnsBoundedStateReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspStateReport: sqlite.RSPStateReport{
			WorkspaceID:    "ws-rsp-state-tool",
			TimeAuthority:  sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-rsp-state-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 5},
			Resolved:       true,
			SignalType:     "AGENT_STATE_POSTERIOR",
			HiddenState:    "FOCUSED",
			RiskBand:       "LOW",
			StateRationale: "Focused and grounded.",
			Summary:        "bounded tool state report",
			LocalAutonomicsCandidates: []sqlite.RSPStateLocalAutonomicsCandidate{
				{Command: "TAKE_BREATH", BoundedLocal: true, Reversible: true},
			},
			GovernedHintSummary: &sqlite.RSPGovernedHintSummary{
				TotalHints: 1,
			},
		},
	}
	tool := living.NewWorkspaceRSPStateReadTool(client, "ws-rsp-state-tool", "agent-rsp-state-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"task_id":"task-rsp-state-tool"}`))
	if err != nil {
		t.Fatalf("rsp_state_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode rsp_state_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-rsp-state-tool" || payload["summary"] != "bounded tool state report" {
		t.Fatalf("expected top-level workspace/summary mirrors, got %+v", payload)
	}
	report, ok := payload["report"].(map[string]any)
	if !ok || report["agent_id"] != "agent-rsp-state-tool" || report["task_id"] != "task-rsp-state-tool" {
		t.Fatalf("expected report scope to inherit current agent and task, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if _, ok := payload["governed_hint_summary"].(map[string]any); !ok {
		t.Fatalf("expected governed_hint_summary mirror, got %+v", payload)
	}
	if candidates, ok := payload["local_autonomics_candidates"].([]any); !ok || len(candidates) != 1 {
		t.Fatalf("expected local_autonomics_candidates mirror, got %+v", payload)
	}
}

func TestWorkspaceRSPForecastReadToolReturnsBoundedForecastReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspForecastReport: sqlite.RSPForecastReport{
			WorkspaceID:             "ws-rsp-forecast-tool",
			TimeAuthority:           sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-rsp-forecast-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 6},
			Resolved:                true,
			SignalType:              "LOAD_FORECAST",
			ForecastReadiness:       "READY",
			ForecastProvenanceHints: []string{"history_backed", "evidence_backed"},
			ForecastCoverageSummary: &sqlite.RSPForecastCoverageSummary{
				ProjectionCount:               1,
				HistoryBackedProjectionCount:  1,
				EvidenceBackedProjectionCount: 1,
			},
			Projections: []sqlite.RSPForecastProjection{
				{Variable: "load", Summary: "bounded forecast projection"},
			},
			Summary: "bounded tool forecast report",
		},
	}
	tool := living.NewWorkspaceRSPForecastReadTool(client, "ws-rsp-forecast-tool", "agent-rsp-forecast-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"task_id":"task-rsp-forecast-tool"}`))
	if err != nil {
		t.Fatalf("rsp_forecast_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode rsp_forecast_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-rsp-forecast-tool" || payload["summary"] != "bounded tool forecast report" {
		t.Fatalf("expected top-level workspace/summary mirrors, got %+v", payload)
	}
	report, ok := payload["report"].(map[string]any)
	if !ok || report["agent_id"] != "agent-rsp-forecast-tool" || report["task_id"] != "task-rsp-forecast-tool" {
		t.Fatalf("expected report scope to inherit current agent and task, got %+v", payload)
	}
	if payload["forecast_readiness"] != "READY" {
		t.Fatalf("expected forecast_readiness mirror, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if _, ok := payload["forecast_coverage_summary"].(map[string]any); !ok {
		t.Fatalf("expected forecast_coverage_summary mirror, got %+v", payload)
	}
	if projections, ok := payload["projections"].([]any); !ok || len(projections) != 1 {
		t.Fatalf("expected projections mirror, got %+v", payload)
	}
}

func TestWorkspaceRSPBeliefReadToolReturnsBoundedBeliefReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspBeliefReport: sqlite.RSPBeliefReport{
			WorkspaceID:            "ws-rsp-belief-tool",
			TimeAuthority:          sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-rsp-belief-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 4},
			Count:                  1,
			LowIndependenceCount:   1,
			HighContradictionCount: 0,
			VerifierStaleCount:     0,
			HighUncertaintyCount:   0,
			Items: []sqlite.RSPBeliefClaimReport{
				{
					ClaimID:              "claim-tool-1",
					ClaimType:            "DECISION_RECORD",
					SourceDiversity:      0.82,
					IndependenceDiscount: 0.91,
					Summary:              "bounded belief item",
				},
			},
			Summary: "bounded tool belief report",
		},
	}
	tool := living.NewWorkspaceRSPBeliefReadTool(client, "ws-rsp-belief-tool", "agent-rsp-belief-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"task_id":"task-rsp-belief-tool"}`))
	if err != nil {
		t.Fatalf("rsp_belief_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode rsp_belief_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-rsp-belief-tool" || payload["summary"] != "bounded tool belief report" {
		t.Fatalf("expected top-level workspace/summary mirrors, got %+v", payload)
	}
	report, ok := payload["report"].(map[string]any)
	if !ok || report["agent_id"] != "agent-rsp-belief-tool" || report["task_id"] != "task-rsp-belief-tool" {
		t.Fatalf("expected report scope to inherit current agent and task, got %+v", payload)
	}
	if payload["count"] != float64(1) || payload["low_independence_count"] != float64(1) {
		t.Fatalf("expected diagnostic mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if items, ok := payload["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("expected items mirror, got %+v", payload)
	}
}

func TestWorkspaceRSPBeliefClaimReadToolReturnsBoundedBeliefItem(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspBeliefClaim: sqlite.RSPBeliefClaimReport{
			WorkspaceID:          "ws-rsp-belief-claim-tool",
			TimeAuthority:        sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-rsp-belief-claim-tool", ReferenceAt: "2026-03-29T00:00:00Z"},
			ClaimID:              "claim-rsp-belief-tool",
			ClaimType:            "FACT",
			Status:               "STABLE",
			SuggestedState:       "STABLE",
			SourceDiversity:      0.66,
			IndependenceDiscount: 0.84,
			IndependentGroups:    2,
			CorrelatedEvidence:   1,
			Summary:              "bounded belief claim tool",
		},
	}
	tool := living.NewWorkspaceRSPBeliefClaimReadTool(client, "ws-rsp-belief-claim-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"claim_id":"claim-rsp-belief-tool"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, `"workspace_id":"ws-rsp-belief-claim-tool"`) ||
		!strings.Contains(out, `"claim_id":"claim-rsp-belief-tool"`) ||
		!strings.Contains(out, `"summary":"bounded belief claim tool"`) ||
		!strings.Contains(out, `"source_diversity":0.66`) {
		t.Fatalf("expected bounded rsp belief claim output, got %s", out)
	}
}

func TestWorkspaceRSPTelemetryReadToolReturnsBoundedTelemetryDump(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspTelemetryDump: sqlite.RSPTelemetryDump{
			Summary: sqlite.RSPTelemetryCalibrationSummary{
				BeliefLogCount:    1,
				AnomalyAlertCount: 2,
				StateLogCount:     1,
				ReadinessBand:     "OBSERVABLE",
				ReadinessCoverageRollup: &sqlite.RSPTelemetryReadinessCoverageRollup{
					OverallReadinessBand:  "OBSERVABLE",
					ObservableStreamCount: 3,
				},
			},
		},
	}
	tool := living.NewWorkspaceRSPTelemetryReadTool(client, "ws-rsp-telemetry-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"limit":16}`))
	if err != nil {
		t.Fatalf("rsp_telemetry_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode rsp_telemetry_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-rsp-telemetry-tool" {
		t.Fatalf("expected top-level workspace mirror, got %+v", payload)
	}
	summary, ok := payload["summary"].(map[string]any)
	if !ok || summary["readiness_band"] != "OBSERVABLE" || summary["anomaly_alert_count"] != float64(2) {
		t.Fatalf("expected summary mirror, got %+v", payload)
	}
	if _, ok := payload["readiness_coverage_rollup"].(map[string]any); !ok {
		t.Fatalf("expected readiness_coverage_rollup mirror, got %+v", payload)
	}
	if _, ok := payload["dump"].(map[string]any); !ok {
		t.Fatalf("expected canonical dump payload, got %+v", payload)
	}
}

func TestWorkspaceUnifiedControlReadToolReturnsBoundedReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		unifiedControlReport: sqlite.UnifiedControlReport{
			WorkspaceID:     "ws-unified-control-tool",
			TimeAuthority:   sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-unified-control-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 9},
			Resolved:        true,
			AdvisoryOnly:    true,
			CapabilityFlags: sqlite.RSPCapabilityFlags{BeliefLive: true, GovernedHintsLive: true},
			AdvisoryControls: sqlite.ControlSuggestedControls{
				FanoutCap:      4,
				ReviewDepth:    2,
				ContextCap:     8,
				MergeThreshold: 5,
				PriorityFocus:  "advisory",
			},
			CandidateControls: sqlite.ControlSuggestedControls{
				FanoutCap:      3,
				ReviewDepth:    2,
				ContextCap:     6,
				MergeThreshold: 4,
				PriorityFocus:  "candidate",
			},
			EffectiveControls: sqlite.ControlSuggestedControls{
				FanoutCap:      2,
				ReviewDepth:    1,
				ContextCap:     4,
				MergeThreshold: 3,
				PriorityFocus:  "effective",
			},
			EffectiveControlsAudit: &sqlite.UnifiedControlEffectiveControlsAudit{
				Found:       true,
				Live:        true,
				ScopeSource: "workspace_fallback",
				Epoch:       7,
				ActorID:     "tester",
			},
			EffectiveControlBasisSummary: &sqlite.UnifiedControlEffectiveControlBasisSummary{
				FieldCount: 6,
			},
			ContradictionSummary: &sqlite.UnifiedControlContradictionSummary{
				TotalCount: 1,
			},
			GovernedHintSummary: &sqlite.RSPGovernedHintSummary{
				TotalHints: 1,
			},
			GovernedHintOutcomes: []sqlite.UnifiedControlGovernedHintOutcome{
				{HintID: "hint-tool-1", ArbitrationOutcome: "ADVISORY_ROUTED"},
			},
			AuditSummary: &sqlite.UnifiedControlAuditSummary{
				AppliedEntryCount: 1,
			},
			AuditCoverage: &sqlite.UnifiedControlAuditCoverage{
				AppliedEntriesWithSourceKinds: 1,
			},
			Summary: "bounded unified control report",
		},
	}
	tool := living.NewWorkspaceUnifiedControlReadTool(client, "ws-unified-control-tool", "agent-unified-control-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"task_id":"task-unified-control-tool"}`))
	if err != nil {
		t.Fatalf("unified_control_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode unified_control_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-unified-control-tool" || payload["summary"] != "bounded unified control report" {
		t.Fatalf("expected top-level workspace/summary mirrors, got %+v", payload)
	}
	if payload["advisory_only"] != true {
		t.Fatalf("expected advisory_only mirror, got %+v", payload)
	}
	if _, ok := payload["capability_flags"].(map[string]any); !ok {
		t.Fatalf("expected capability_flags mirror, got %+v", payload)
	}
	if advisory, ok := payload["advisory_controls"].(map[string]any); !ok || advisory["priority_focus"] != "advisory" {
		t.Fatalf("expected advisory_controls mirror, got %+v", payload)
	}
	if candidate, ok := payload["candidate_controls"].(map[string]any); !ok || candidate["priority_focus"] != "candidate" {
		t.Fatalf("expected candidate_controls mirror, got %+v", payload)
	}
	if _, ok := payload["effective_controls"].(map[string]any); !ok {
		t.Fatalf("expected effective_controls mirror, got %+v", payload)
	}
	if audit, ok := payload["effective_controls_audit"].(map[string]any); !ok || audit["scope_source"] != "workspace_fallback" || audit["live"] != true {
		t.Fatalf("expected effective_controls_audit mirror, got %+v", payload)
	}
	if _, ok := payload["effective_control_basis_summary"].(map[string]any); !ok {
		t.Fatalf("expected effective_control_basis_summary mirror, got %+v", payload)
	}
	if contradictionSummary, ok := payload["contradiction_summary"].(map[string]any); !ok || contradictionSummary["total_count"] != float64(1) {
		t.Fatalf("expected contradiction_summary mirror, got %+v", payload)
	}
	if _, ok := payload["audit_summary"].(map[string]any); !ok {
		t.Fatalf("expected audit_summary mirror, got %+v", payload)
	}
	if _, ok := payload["report"].(map[string]any); !ok {
		t.Fatalf("expected canonical unified control report payload, got %+v", payload)
	}
}

func TestWorkspaceControlReportReadToolReturnsBoundedReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		controlReport: sqlite.ControlReport{
			WorkspaceID:   "ws-control-report-tool",
			TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-control-report-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 8},
			Workspace: sqlite.ControlWorkspaceMetrics{
				TotalClusters:            1,
				HotClusterCount:          1,
				AttentionClusterCount:    1,
				HighestPressureClusterID: "proto-cluster-tool-1",
				HighestPressureScore:     2,
			},
			Clusters: []sqlite.ControlClusterReport{
				{
					ProtoClusterID: "proto-cluster-tool-1",
					Signals: sqlite.ControlSignalVector{
						PressureScore: 2,
						AttentionBand: "WATCH",
					},
					Summary: "bounded control report tool cluster",
				},
			},
		},
	}
	tool := living.NewWorkspaceControlReportReadTool(client, "ws-control-report-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("control_report_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode control_report_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-control-report-tool" {
		t.Fatalf("expected workspace mirror, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if workspace, ok := payload["workspace"].(map[string]any); !ok || workspace["total_clusters"] != float64(1) {
		t.Fatalf("expected workspace metrics mirror, got %+v", payload)
	}
	if clusters, ok := payload["clusters"].([]any); !ok || len(clusters) != 1 {
		t.Fatalf("expected clusters mirror, got %+v", payload)
	}
}

func TestWorkspaceControlClusterReadToolReturnsBoundedDetail(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		controlClusterDetail: sqlite.ControlClusterDetail{
			TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-control-cluster-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 8},
			Cluster: sqlite.ControlClusterReport{
				ProtoClusterID: "proto-cluster-tool-1",
				Signals: sqlite.ControlSignalVector{
					PressureScore: 2,
					AttentionBand: "WATCH",
				},
				Summary: "bounded control cluster tool",
			},
			Tensions: []sqlite.TensionRecord{
				{TensionID: "tension-tool-1", ProtoClusterID: "proto-cluster-tool-1", TensionType: "bottleneck"},
			},
		},
	}
	tool := living.NewWorkspaceControlClusterReadTool(client, "ws-control-cluster-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"proto_cluster_id":"proto-cluster-tool-1"}`))
	if err != nil {
		t.Fatalf("control_cluster_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode control_cluster_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-control-cluster-tool" || payload["proto_cluster_id"] != "proto-cluster-tool-1" {
		t.Fatalf("expected workspace/cluster mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if _, ok := payload["cluster"].(map[string]any); !ok {
		t.Fatalf("expected cluster mirror, got %+v", payload)
	}
	if tensions, ok := payload["tensions"].([]any); !ok || len(tensions) != 1 {
		t.Fatalf("expected tensions mirror, got %+v", payload)
	}
}

func TestWorkspaceControlStateReadToolReturnsBoundedReport(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		controlStateReport: sqlite.ClusterControlStateReport{
			WorkspaceID:   "ws-control-state-tool",
			TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-control-state-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 8},
			Workspace: sqlite.ClusterControlStateWorkspaceMetrics{
				TotalClusters:            1,
				HotClusterCount:          1,
				HighestPressureClusterID: "proto-cluster-tool-1",
				HighestPressureScore:     2,
			},
			Clusters: []sqlite.ClusterControlStateCluster{
				{
					ProtoClusterID: "proto-cluster-tool-1",
					State: sqlite.ClusterControlStateRecord{
						CurrentMode:   "COHERENCE",
						AttentionBand: "ACTIVE",
						PressureScore: 2,
						Summary:       "bounded control state tool cluster",
					},
				},
			},
		},
	}
	tool := living.NewWorkspaceControlStateReadTool(client, "ws-control-state-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"mode":"COHERENCE"}`))
	if err != nil {
		t.Fatalf("control_state_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode control_state_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-control-state-tool" {
		t.Fatalf("expected workspace mirror, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if workspace, ok := payload["workspace"].(map[string]any); !ok || workspace["total_clusters"] != float64(1) {
		t.Fatalf("expected workspace metrics mirror, got %+v", payload)
	}
	if clusters, ok := payload["clusters"].([]any); !ok || len(clusters) != 1 {
		t.Fatalf("expected clusters mirror, got %+v", payload)
	}
}

func TestWorkspaceControlStateClusterReadToolReturnsBoundedDetail(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		controlStateDetail: sqlite.ClusterControlStateDetail{
			TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-control-state-cluster-tool", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 9},
			Cluster: sqlite.ControlClusterReport{
				ProtoClusterID: "proto-cluster-tool-1",
				Summary:        "bounded control state cluster detail",
			},
			State: sqlite.ClusterControlStateCluster{
				ProtoClusterID: "proto-cluster-tool-1",
				State: sqlite.ClusterControlStateRecord{
					CurrentMode:     "COHERENCE",
					AttentionBand:   "ACTIVE",
					PressureScore:   2,
					CandidateStreak: 1,
					Summary:         "bounded control state detail",
				},
			},
			Tensions: []sqlite.TensionRecord{
				{TensionID: "tension-tool-1", ProtoClusterID: "proto-cluster-tool-1"},
			},
			Events: []sqlite.RuntimeEventRecord{
				{EventID: "rtev-tool-1", EventType: "cluster.control_state_ticked", EntityID: "proto-cluster-tool-1"},
			},
		},
	}
	tool := living.NewWorkspaceControlStateClusterReadTool(client, "ws-control-state-cluster-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{"proto_cluster_id":"proto-cluster-tool-1"}`))
	if err != nil {
		t.Fatalf("control_state_cluster_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode control_state_cluster_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-control-state-cluster-tool" || payload["proto_cluster_id"] != "proto-cluster-tool-1" {
		t.Fatalf("expected workspace/proto cluster mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if _, ok := payload["cluster_basis"].(map[string]any); !ok {
		t.Fatalf("expected cluster_basis mirror, got %+v", payload)
	}
	if _, ok := payload["state"].(map[string]any); !ok {
		t.Fatalf("expected state mirror, got %+v", payload)
	}
	if tensions, ok := payload["tensions"].([]any); !ok || len(tensions) != 1 {
		t.Fatalf("expected tensions mirror, got %+v", payload)
	}
}

func TestWorkspaceCompactionCandidatesReadToolReturnsBoundedCandidates(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-compaction-candidates-tool"
		agentID     = "agent-compaction-candidates-tool"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Compaction Candidates Tool Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Compaction Candidates Tool Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-compaction-tool-1",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-compaction-tool-1",
		StartedAt:   "2026-03-30T00:00:00Z",
	}); err != nil {
		t.Fatalf("create candidate session: %v", err)
	}
	if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
		SessionID:   "sess-compaction-tool-1",
		Sequence:    1,
		Role:        "assistant",
		ContentJSON: `{"type":"message","content":"large candidate payload"}`,
		TokenCount:  13000,
	}); err != nil {
		t.Fatalf("append candidate session message: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-compaction-tool-2",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-compaction-tool-2",
		StartedAt:   "2026-03-30T00:10:00Z",
	}); err != nil {
		t.Fatalf("create non-candidate session: %v", err)
	}
	if err := store.AppendAgentSessionMessage(ctx, sqlite.AgentSessionMessageInput{
		SessionID:   "sess-compaction-tool-2",
		Sequence:    1,
		Role:        "assistant",
		ContentJSON: `{"type":"message","content":"small payload"}`,
		TokenCount:  10,
	}); err != nil {
		t.Fatalf("append non-candidate session message: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceCompactionCandidatesReadTool(client, workspaceID, agentID)

	out, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("compaction_candidates_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode compaction_candidates_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID || payload["agent_id"] != agentID {
		t.Fatalf("expected workspace/agent mirrors, got %+v", payload)
	}
	if payload["active_only"] != true || payload["min_messages"] != float64(12) || payload["min_tokens"] != float64(12000) {
		t.Fatalf("expected canonical threshold mirrors, got %+v", payload)
	}
	if payload["count"] != float64(1) {
		t.Fatalf("expected exactly one compaction candidate, got %+v", payload)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one bounded candidate item, got %+v", payload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["session_id"] != "sess-compaction-tool-1" || item["message_tokens"] != float64(13000) {
		t.Fatalf("expected surfaced canonical compaction candidate, got %+v", items[0])
	}
}

func TestWorkspaceCompactionSnapshotsReadToolReturnsBoundedSnapshots(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-compaction-snapshots-tool"
		agentID     = "agent-compaction-snapshots-tool"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Compaction Snapshots Tool Workspace",
		CreatedBy:   "test-user",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "test-user",
		DisplayName: "Compaction Snapshots Tool Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   "sess-compaction-snapshot-tool-1",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      "task-compaction-snapshot-tool-1",
		StartedAt:   "2026-03-30T00:00:00Z",
	}); err != nil {
		t.Fatalf("create snapshot session: %v", err)
	}
	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, sqlite.SessionCompactionSnapshotInput{
		SnapshotID:          "compaction-snapshot-tool-1",
		SessionID:           "sess-compaction-snapshot-tool-1",
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TriggerKind:         "token_budget_exceeded",
		TokenBudget:         12000,
		MessageCountBefore:  18,
		MessageCountAfter:   4,
		MessageTokensBefore: 13000,
		MessageTokensAfter:  2000,
		TotalInputTokens:    7000,
		TotalOutputTokens:   6000,
		SummaryText:         "bounded compaction snapshot",
	})
	if err != nil {
		t.Fatalf("record compaction snapshot: %v", err)
	}

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	tool := living.NewWorkspaceCompactionSnapshotsReadTool(client, workspaceID, agentID)

	out, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("compaction_snapshots_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode compaction_snapshots_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID || payload["agent_id"] != agentID {
		t.Fatalf("expected workspace/agent mirrors, got %+v", payload)
	}
	if payload["count"] != float64(1) {
		t.Fatalf("expected exactly one compaction snapshot, got %+v", payload)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one bounded snapshot item, got %+v", payload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["snapshot_id"] != snapshot.SnapshotID || item["session_id"] != snapshot.SessionID {
		t.Fatalf("expected surfaced canonical compaction snapshot, got %+v", items[0])
	}
	if item["summary_text"] != "bounded compaction snapshot" || item["canonical_memory_id"] == "" {
		t.Fatalf("expected bounded compaction snapshot mirrors, got %+v", items[0])
	}
}

func TestWorkspaceEventsListReadToolReturnsBoundedEvents(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-list-tool"
		agentID     = "agent-events-list-tool"
		sessionID   = "sess-events-list-tool-1"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  sessionID,
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "bounded events list start",
		Status:     model.SessionStatusActive,
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("record events list start event: %v", err)
	}

	tool := living.NewWorkspaceEventsListReadTool(client, workspaceID, agentID)
	out, err := tool.Execute(ctx, json.RawMessage(`{"session_id":"sess-events-list-tool-1","event_type":"session.start"}`))
	if err != nil {
		t.Fatalf("events_list_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode events_list_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID || payload["count"] != float64(1) {
		t.Fatalf("expected workspace/count mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	items, ok := payload["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one bounded runtime event item, got %+v", payload)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["session_id"] != sessionID || item["event_type"] != model.SessionEventStart {
		t.Fatalf("expected surfaced canonical runtime event, got %+v", items[0])
	}
}

func TestWorkspaceEventsReplayReadToolReturnsBoundedReport(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-replay-tool"
		agentID     = "agent-events-replay-tool"
		sessionID   = "sess-events-replay-tool-1"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  sessionID,
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "bounded replay start",
		Status:     model.SessionStatusActive,
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("record replay start event: %v", err)
	}
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType: model.SessionEventEnd,
		SessionID: sessionID,
		AgentID:   agentID,
		TaskID:    taskID,
		Summary:   "bounded replay ended",
		Status:    model.SessionStatusEnded,
	}); err != nil {
		t.Fatalf("record replay end event: %v", err)
	}

	tool := living.NewWorkspaceEventsReplayReadTool(client, workspaceID, agentID)
	out, err := tool.Execute(ctx, json.RawMessage(`{"session_id":"sess-events-replay-tool-1","include_events":true}`))
	if err != nil {
		t.Fatalf("events_replay_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode events_replay_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID {
		t.Fatalf("expected workspace mirror, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	evaluation, ok := payload["evaluation"].(map[string]any)
	if !ok || strings.TrimSpace(evaluation["verdict"].(string)) == "" {
		t.Fatalf("expected evaluation mirror, got %+v", payload)
	}
	counts, ok := payload["counts"].(map[string]any)
	if !ok || counts["sessions"] != float64(1) || counts["events"] == float64(0) {
		t.Fatalf("expected replay counts mirror, got %+v", payload)
	}
	if _, ok := payload["report"].(map[string]any); !ok {
		t.Fatalf("expected canonical replay report payload, got %+v", payload)
	}
}

func TestWorkspaceEventsEvaluateReadToolReturnsBoundedEvaluation(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-events-evaluate-tool"
		agentID     = "agent-events-evaluate-tool"
		sessionID   = "sess-events-evaluate-tool-1"
	)
	taskID := seedWorkspaceAndTask(t, ctx, store, workspaceID, agentID, "generic")
	claimAndRunLivingTestTask(t, ctx, store, workspaceID, taskID, agentID)

	client := living.NewDirectRhizomeClient(store, workspaceID)
	client.SetAgentID(agentID)
	if err := client.RecordSessionEvent(ctx, living.SessionEventInput{
		EventType:  model.SessionEventStart,
		SessionID:  sessionID,
		AgentID:    agentID,
		TaskID:     taskID,
		Summary:    "bounded evaluate start",
		Status:     model.SessionStatusActive,
		OwnerScope: "task/session",
	}); err != nil {
		t.Fatalf("record evaluate start event: %v", err)
	}

	tool := living.NewWorkspaceEventsEvaluateReadTool(client, workspaceID, agentID)
	out, err := tool.Execute(ctx, json.RawMessage(`{"session_id":"sess-events-evaluate-tool-1"}`))
	if err != nil {
		t.Fatalf("events_evaluate_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode events_evaluate_read output: %v", err)
	}
	if payload["workspace_id"] != workspaceID || payload["truncated"] != false {
		t.Fatalf("expected workspace/truncated mirrors, got %+v", payload)
	}
	if _, ok := payload["time_authority"].(map[string]any); !ok {
		t.Fatalf("expected time_authority mirror, got %+v", payload)
	}
	if _, ok := payload["filter"].(map[string]any); !ok {
		t.Fatalf("expected filter mirror, got %+v", payload)
	}
	evaluation, ok := payload["evaluation"].(map[string]any)
	if !ok || strings.TrimSpace(evaluation["verdict"].(string)) == "" {
		t.Fatalf("expected evaluation mirror, got %+v", payload)
	}
	counts, ok := payload["counts"].(map[string]any)
	if !ok || counts["sessions"] != float64(1) {
		t.Fatalf("expected evaluation counts mirror, got %+v", payload)
	}
	if _, hasReport := payload["report"]; hasReport {
		t.Fatalf("expected evaluate facade to stay bounded without replay report, got %+v", payload)
	}
}

func TestWorkspaceRSPCapabilityReadToolReturnsFlags(t *testing.T) {
	t.Parallel()

	client := &mockRhizomeForBrain{
		rspCapabilityFlags: sqlite.RSPCapabilityFlags{
			BeliefLive:        true,
			AnomalyShadow:     true,
			StateShadow:       true,
			GovernedHintsLive: true,
		},
	}
	tool := living.NewWorkspaceRSPCapabilityReadTool(client, "ws-rsp-capability-tool")

	out, err := tool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_capability_read execute failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode rsp_capability_read output: %v", err)
	}
	if payload["workspace_id"] != "ws-rsp-capability-tool" {
		t.Fatalf("expected workspace mirror, got %+v", payload)
	}
	flags, ok := payload["capability_flags"].(map[string]any)
	if !ok || flags["belief_live"] != true || flags["governed_hints_live"] != true {
		t.Fatalf("expected capability_flags mirror, got %+v", payload)
	}
}
