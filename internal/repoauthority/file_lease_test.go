package repoauthority

import (
	"strings"
	"testing"
	"time"
)

func TestFileLeaseAcquireAuthorizeRenewAndRelease(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)

	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-b1-4", []string{"a.go", "b.go"}),
		TTL:     10 * time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Schema != FileLeaseSchemaVersion {
		t.Fatalf("schema = %q", lease.Schema)
	}
	if lease.Status != FileLeaseStatusActive {
		t.Fatalf("status = %q", lease.Status)
	}
	if lease.Term <= 0 {
		t.Fatalf("term = %d", lease.Term)
	}
	if len(lease.Pathset) != 2 || lease.Pathset[0] != "a.go" || lease.Pathset[1] != "b.go" {
		t.Fatalf("unexpected pathset %#v", lease.Pathset)
	}

	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  HolderForLease(lease),
		Paths:   []string{"b.go"},
		Now:     now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AuthorizeMutation: %v", err)
	}

	renewed, err := store.Renew(RenewFileLeaseInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  HolderForLease(lease),
		TTL:     20 * time.Minute,
		Now:     now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if renewed.Term != lease.Term {
		t.Fatalf("renew changed term: got %d want %d", renewed.Term, lease.Term)
	}
	if renewed.ExpiresAt == lease.ExpiresAt {
		t.Fatalf("renew did not extend expiry: %q", renewed.ExpiresAt)
	}

	released, err := store.Release(ReleaseFileLeaseInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  HolderForLease(lease),
		Now:     now.Add(6 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if released.Status != FileLeaseStatusReleased {
		t.Fatalf("release status = %q", released.Status)
	}
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  HolderForLease(lease),
		Paths:   []string{"a.go"},
		Now:     now.Add(7 * time.Minute),
	}); err == nil || !strings.Contains(err.Error(), "released") {
		t.Fatalf("released lease authorize error = %v, want released", err)
	}
}

func TestFileLeaseRejectsOverlappingLiveLease(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	first, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-a", []string{"shared.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	_, err = store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-b", []string{"shared.go"}),
		TTL:     time.Minute,
		Now:     now.Add(30 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "already leased") || !strings.Contains(err.Error(), first.ID) {
		t.Fatalf("second Acquire error = %v, want already leased by first lease", err)
	}
}

func TestFileLeaseExpiredHolderCannotAuthorizeAndNewTermWins(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	oldLease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-old", []string{"shared.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire old: %v", err)
	}

	expiredAt := now.Add(time.Minute)
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: oldLease.ID,
		Term:    oldLease.Term,
		Holder:  HolderForLease(oldLease),
		Paths:   []string{"shared.go"},
		Now:     expiredAt,
	}); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired authorize error = %v, want expired", err)
	}

	newLease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-new", []string{"shared.go"}),
		TTL:     time.Minute,
		Now:     expiredAt.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Acquire new after expiry: %v", err)
	}
	if newLease.Term <= oldLease.Term {
		t.Fatalf("new term = %d, want greater than old %d", newLease.Term, oldLease.Term)
	}
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: oldLease.ID,
		Term:    oldLease.Term,
		Holder:  HolderForLease(oldLease),
		Paths:   []string{"shared.go"},
		Now:     expiredAt.Add(2 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("old stale authorize error = %v, want expired", err)
	}
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: newLease.ID,
		Term:    oldLease.Term,
		Holder:  HolderForLease(newLease),
		Paths:   []string{"shared.go"},
		Now:     expiredAt.Add(2 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "term mismatch") {
		t.Fatalf("stale term authorize error = %v, want term mismatch", err)
	}
}

func TestFileLeaseRejectsWrongHolderPrincipalAndPath(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-a", []string{"owned.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	tests := []struct {
		name  string
		input AuthorizeFileMutationInput
		want  string
	}{
		{
			name: "wrong agent",
			input: AuthorizeFileMutationInput{
				LeaseID: lease.ID,
				Term:    lease.Term,
				Holder:  mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) { h.AgentID = "agent-b" }),
				Paths:   []string{"owned.go"},
				Now:     now,
			},
			want: "agent mismatch",
		},
		{
			name: "wrong principal",
			input: AuthorizeFileMutationInput{
				LeaseID: lease.ID,
				Term:    lease.Term,
				Holder:  mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) { h.Principal = PrincipalRef{Type: "agent", ID: "other-principal"} }),
				Paths:   []string{"owned.go"},
				Now:     now,
			},
			want: "principal mismatch",
		},
		{
			name: "outside path",
			input: AuthorizeFileMutationInput{
				LeaseID: lease.ID,
				Term:    lease.Term,
				Holder:  HolderForLease(lease),
				Paths:   []string{"outside.go"},
				Now:     now,
			},
			want: "not covered",
		},
		{
			name: "traversal path",
			input: AuthorizeFileMutationInput{
				LeaseID: lease.ID,
				Term:    lease.Term,
				Holder:  HolderForLease(lease),
				Paths:   []string{"../owned.go"},
				Now:     now,
			},
			want: "escapes repo root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.AuthorizeMutation(tt.input)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("AuthorizeMutation error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestFileLeaseRejectsStaleSameAgentHolderIdentity(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-a", []string{"owned.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	wrongSession := mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) {
		h.SessionID = "stale-session"
	})
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  wrongSession,
		Paths:   []string{"owned.go"},
		Now:     now,
	}); err == nil || !strings.Contains(err.Error(), "session mismatch") {
		t.Fatalf("wrong session authorize error = %v, want session mismatch", err)
	}

	wrongRun := mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) {
		h.RunID = "stale-run"
	})
	if _, err := store.Renew(RenewFileLeaseInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  wrongRun,
		TTL:     time.Minute,
		Now:     now,
	}); err == nil || !strings.Contains(err.Error(), "run mismatch") {
		t.Fatalf("wrong run renew error = %v, want run mismatch", err)
	}

	wrongCapability := mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) {
		h.CapabilitySnapshotID = "stale-capability"
	})
	if _, err := store.Release(ReleaseFileLeaseInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  wrongCapability,
		Now:     now,
	}); err == nil || !strings.Contains(err.Error(), "capability snapshot mismatch") {
		t.Fatalf("wrong capability release error = %v, want capability snapshot mismatch", err)
	}

	wrongDigest := mutateHolder(HolderForLease(lease), func(h *FileLeaseHolder) {
		h.ContextDigest = "sha256:stale"
	})
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  wrongDigest,
		Paths:   []string{"owned.go"},
		Now:     now,
	}); err == nil || !strings.Contains(err.Error(), "context digest mismatch") {
		t.Fatalf("wrong digest authorize error = %v, want context digest mismatch", err)
	}
}

func TestFileLeaseRevocationStopsOriginalSameAgentHolder(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	oldLease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-a", []string{"owned.go"}),
		TTL:     time.Hour,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire old: %v", err)
	}
	oldHolder := HolderForLease(oldLease)

	revoked, err := store.RevokeLeasesForHolder(RevokeFileLeasesInput{
		Holder: oldHolder,
		Now:    now.Add(time.Minute),
		Reason: "session takeover",
	})
	if err != nil {
		t.Fatalf("RevokeLeasesForHolder: %v", err)
	}
	if revoked.Revoked != 1 {
		t.Fatalf("revoked count = %d, want 1", revoked.Revoked)
	}
	if len(revoked.Leases) != 1 || revoked.Leases[0].Status != FileLeaseStatusRevoked {
		t.Fatalf("revoked leases = %#v, want revoked lease", revoked.Leases)
	}

	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: oldLease.ID,
		Term:    oldLease.Term,
		Holder:  oldHolder,
		Paths:   []string{"owned.go"},
		Now:     now.Add(2 * time.Minute),
	}); err == nil || !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("revoked holder authorize error = %v, want revoked", err)
	}
	if _, err := store.Renew(RenewFileLeaseInput{
		LeaseID: oldLease.ID,
		Term:    oldLease.Term,
		Holder:  oldHolder,
		TTL:     time.Hour,
		Now:     now.Add(2 * time.Minute),
	}); err == nil || !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("revoked holder renew error = %v, want revoked", err)
	}

	newLease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContextWithRefs("agent-a", "task-takeover", "session-takeover", "run-takeover", "cap-takeover", []string{"owned.go"}),
		TTL:     time.Hour,
		Now:     now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Acquire new after revoke: %v", err)
	}
	if newLease.Term <= oldLease.Term {
		t.Fatalf("new term = %d, want greater than old %d", newLease.Term, oldLease.Term)
	}
	if newLease.ID == oldLease.ID {
		t.Fatalf("new lease reused old id %q", newLease.ID)
	}
}

func TestFileLeaseReturnedPathsetMutationCannotExpandAuthorization(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-alias", []string{"owned.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	holder := HolderForLease(lease)
	lease.Pathset[0] = "evil.go"

	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  holder,
		Paths:   []string{"evil.go"},
		Now:     now.Add(time.Second),
	}); err == nil || !strings.Contains(err.Error(), "not covered") {
		t.Fatalf("AuthorizeMutation after returned pathset mutation = %v, want not covered", err)
	}
	if err := store.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  holder,
		Paths:   []string{"owned.go"},
		Now:     now.Add(time.Second),
	}); err != nil {
		t.Fatalf("AuthorizeMutation for original path after returned pathset mutation: %v", err)
	}
}

func TestFileLeaseReturnedPathsetMutationCannotPoisonReleaseCleanup(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-alias", []string{"owned.go"}),
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	holder := HolderForLease(lease)
	lease.Pathset[0] = "evil.go"

	if _, err := store.Release(ReleaseFileLeaseInput{
		LeaseID: lease.ID,
		Term:    lease.Term,
		Holder:  holder,
		Now:     now.Add(time.Second),
	}); err != nil {
		t.Fatalf("Release after returned pathset mutation: %v", err)
	}
	next, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContextWithRefs("agent-next", "task-next", "session-next", "run-next", "cap-next", []string{"owned.go"}),
		TTL:     time.Minute,
		Now:     now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("Acquire after release cleanup: %v", err)
	}
	if next.Term <= lease.Term {
		t.Fatalf("next term = %d, want greater than old %d", next.Term, lease.Term)
	}
}

func TestFileLeaseRejectsInvalidAcquireInputs(t *testing.T) {
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)

	_, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLeaseContext("agent-b1-4", []string{"a.go"}),
		TTL:     0,
		Now:     now,
	})
	if err == nil || !strings.Contains(err.Error(), "ttl must be positive") {
		t.Fatalf("zero ttl error = %v, want ttl", err)
	}

	withLease := preLeaseContext("agent-b1-4", []string{"a.go"})
	withLease.Lease = LeaseRef{ID: "already-held", Term: 1}
	_, err = store.Acquire(AcquireFileLeaseInput{
		Context: withLease,
		TTL:     time.Minute,
		Now:     now,
	})
	if err == nil || !strings.Contains(err.Error(), "must not already attach lease") {
		t.Fatalf("attached lease acquire error = %v, want attached lease reject", err)
	}
}

func mutateHolder(holder FileLeaseHolder, mutate func(*FileLeaseHolder)) FileLeaseHolder {
	mutate(&holder)
	return holder
}

func preLeaseContext(agentID string, pathset []string) Context {
	return preLeaseContextWithRefs(agentID, "task-b1-4", "session-b1-4", "run-b1-4", "cap-b1-4", pathset)
}

func preLeaseContextWithRefs(agentID, taskID, sessionID, runID, capabilitySnapshotID string, pathset []string) Context {
	normalized, err := NormalizePathSet(pathset)
	if err != nil {
		panic(err)
	}
	hashes := make(map[string]string, len(normalized))
	for _, p := range normalized {
		hashes[p] = "sha256:base-" + strings.ReplaceAll(p, "/", "-")
	}
	return Context{
		Mode:        ModePatchOnlyTempRepo,
		WorkspaceID: "ws-b1-4",
		TaskID:      taskID,
		SessionID:   sessionID,
		RunID:       runID,
		AgentID:     agentID,
		Principal: PrincipalRef{
			Type: "agent",
			ID:   agentID,
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     capabilitySnapshotID,
			Schema: "runtime_capability_snapshot.v1",
		},
		RepoRoot: "C:/work/rhizome",
		Base: BaseIdentity{
			Ref:        "base-b1-4",
			TreeHash:   "sha256:base-tree",
			FileHashes: hashes,
		},
		Pathset: normalized,
	}
}
