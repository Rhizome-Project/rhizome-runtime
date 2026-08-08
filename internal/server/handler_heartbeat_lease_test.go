package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAgentHeartbeatLeaseRPCAcquireConflictAndRelease(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-heartbeat-lease"
	agentID := "agent-ui"
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, agentID)
	ctx := testAuthContext(workspaceID, "agent", agentID)

	raw, err := json.Marshal(agentHeartbeatLeaseAcquireParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-a",
		LeaseToken:  "token-a",
		Locks:       []string{"browser"},
		TTLSec:      60,
	})
	if err != nil {
		t.Fatalf("marshal acquire: %v", err)
	}
	result, rpcErr := h.agentHeartbeatLeaseAcquire(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentHeartbeatLeaseAcquire rpc error: %+v", rpcErr)
	}
	envelope := result.(map[string]any)
	lease := envelope["lease"].(sqlite.AgentHeartbeatLeaseResult)
	if !lease.Acquired || envelope["status"] != "ACQUIRED" {
		t.Fatalf("expected acquired lease, got %#v", result)
	}

	raw, err = json.Marshal(agentHeartbeatLeaseAcquireParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: "global_reflection",
		OwnerID:     "runtime-b",
		LeaseToken:  "token-b",
		Locks:       []string{"browser"},
		TTLSec:      60,
	})
	if err != nil {
		t.Fatalf("marshal conflicting acquire: %v", err)
	}
	result, rpcErr = h.agentHeartbeatLeaseAcquire(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("agentHeartbeatLeaseAcquire conflict rpc error: %+v", rpcErr)
	}
	envelope = result.(map[string]any)
	lease = envelope["lease"].(sqlite.AgentHeartbeatLeaseResult)
	if lease.Acquired || lease.ConflictReason != "lock_already_leased" || lease.ConflictLeaseToken != "" || envelope["status"] != "CONFLICT" {
		t.Fatalf("expected lock conflict, got %#v", result)
	}

	wrongReleaseRaw, err := json.Marshal(agentHeartbeatLeaseReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: "visual_audit",
		LeaseToken:  "wrong-token",
	})
	if err != nil {
		t.Fatalf("marshal wrong-token release: %v", err)
	}
	result, rpcErr = h.agentHeartbeatLeaseRelease(ctx, wrongReleaseRaw)
	if rpcErr != nil {
		t.Fatalf("agentHeartbeatLeaseRelease wrong token rpc error: %+v", rpcErr)
	}
	if release := result.(map[string]any); release["released"].(bool) || release["status"] != "NOOP" {
		t.Fatalf("expected no-op release status, got %#v", result)
	}

	releaseRaw, err := json.Marshal(agentHeartbeatLeaseReleaseParams{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		HeartbeatID: "visual_audit",
		LeaseToken:  "token-a",
	})
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	result, rpcErr = h.agentHeartbeatLeaseRelease(ctx, releaseRaw)
	if rpcErr != nil {
		t.Fatalf("agentHeartbeatLeaseRelease rpc error: %+v", rpcErr)
	}
	if released := result.(map[string]any)["released"].(bool); !released {
		t.Fatalf("expected release, got %#v", result)
	}
}

func TestAgentHeartbeatLeaseRPCAuthorizesAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	workspaceID := "ws-heartbeat-auth"
	ensureMessagingTestWorkspaceAndAgents(t, store, workspaceID, "agent-a", "agent-b")

	raw, err := json.Marshal(agentHeartbeatLeaseAcquireParams{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-a",
		LeaseToken:  "token-a",
		Locks:       []string{"browser"},
		TTLSec:      60,
	})
	if err != nil {
		t.Fatalf("marshal acquire: %v", err)
	}
	_, rpcErr := h.agentHeartbeatLeaseAcquire(testAuthContext(workspaceID, "agent", "agent-b"), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied || !strings.Contains(rpcErr.Message, "actor mismatch") {
		t.Fatalf("expected actor mismatch, got %+v", rpcErr)
	}
	_, rpcErr = h.agentHeartbeatLeaseAcquire(context.Background(), raw)
	if rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected unauthorized, got %+v", rpcErr)
	}
}
