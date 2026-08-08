package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestAgentHeartbeatLeaseAcquireConflictExpiryAndRelease(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	defer store.Close()

	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	first, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-a",
		LeaseToken:  "token-a",
		Locks:       []string{"browser", "browser", " render "},
		TTL:         10 * time.Second,
		Now:         now,
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease first: %v", err)
	}
	if !first.Acquired {
		t.Fatalf("expected first acquire, got %+v", first)
	}
	if len(first.Locks) != 2 || first.Locks[0] != "browser" || first.Locks[1] != "render" {
		t.Fatalf("expected normalized locks, got %#v", first.Locks)
	}

	second, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-b",
		LeaseToken:  "token-b",
		Locks:       []string{"browser"},
		TTL:         10 * time.Second,
		Now:         now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease conflict: %v", err)
	}
	if second.Acquired || second.ConflictReason != "heartbeat_already_leased" || second.ConflictLeaseToken != "" {
		t.Fatalf("expected heartbeat conflict, got %+v", second)
	}

	otherHeartbeat, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "global_reflection",
		OwnerID:     "runtime-b",
		LeaseToken:  "token-b",
		Locks:       []string{"browser"},
		TTL:         10 * time.Second,
		Now:         now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease lock conflict: %v", err)
	}
	if otherHeartbeat.Acquired || otherHeartbeat.ConflictReason != "lock_already_leased" || otherHeartbeat.ConflictLock != "browser" {
		t.Fatalf("expected lock conflict, got %+v", otherHeartbeat)
	}

	released, err := store.ReleaseAgentHeartbeatLease(ctx, AgentHeartbeatLeaseReleaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "visual_audit",
		LeaseToken:  "wrong-token",
	})
	if err != nil {
		t.Fatalf("ReleaseAgentHeartbeatLease wrong token: %v", err)
	}
	if released {
		t.Fatalf("expected wrong token release to be a no-op")
	}

	afterExpiry, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "global_reflection",
		OwnerID:     "runtime-b",
		LeaseToken:  "token-b",
		Locks:       []string{"browser"},
		TTL:         10 * time.Second,
		Now:         now.Add(11 * time.Second),
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease after expiry: %v", err)
	}
	if !afterExpiry.Acquired {
		t.Fatalf("expected acquire after expiry, got %+v", afterExpiry)
	}

	released, err = store.ReleaseAgentHeartbeatLease(ctx, AgentHeartbeatLeaseReleaseInput{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		HeartbeatID: "global_reflection",
		LeaseToken:  "token-b",
	})
	if err != nil {
		t.Fatalf("ReleaseAgentHeartbeatLease: %v", err)
	}
	if !released {
		t.Fatalf("expected matching token release")
	}
}

func TestAgentHeartbeatLeaseRefreshWithoutLocksPreservesExistingLocks(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	defer store.Close()

	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	first, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-refresh",
		AgentID:     "agent-ui",
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-a",
		LeaseToken:  "token-a",
		Locks:       []string{"browser"},
		TTL:         time.Minute,
		Now:         now,
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease first: %v", err)
	}
	if !first.Acquired {
		t.Fatalf("expected first acquire, got %+v", first)
	}

	refreshed, err := store.RefreshAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-refresh",
		AgentID:     "agent-ui",
		HeartbeatID: "visual_audit",
		OwnerID:     "runtime-a",
		LeaseToken:  "token-a",
		TTL:         time.Minute,
		Now:         now.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("RefreshAgentHeartbeatLease: %v", err)
	}
	if !refreshed.Acquired || len(refreshed.Locks) != 1 || refreshed.Locks[0] != "browser" {
		t.Fatalf("expected refresh to preserve browser lock, got %+v", refreshed)
	}

	conflict, err := store.AcquireAgentHeartbeatLease(ctx, AgentHeartbeatLeaseInput{
		WorkspaceID: "ws-refresh",
		AgentID:     "agent-ui",
		HeartbeatID: "global_reflection",
		OwnerID:     "runtime-b",
		LeaseToken:  "token-b",
		Locks:       []string{"browser"},
		TTL:         time.Minute,
		Now:         now.Add(20 * time.Second),
	})
	if err != nil {
		t.Fatalf("AcquireAgentHeartbeatLease after refresh: %v", err)
	}
	if conflict.Acquired || conflict.ConflictReason != "lock_already_leased" || conflict.ConflictLeaseToken != "" {
		t.Fatalf("expected preserved lock conflict without token disclosure, got %+v", conflict)
	}
}
