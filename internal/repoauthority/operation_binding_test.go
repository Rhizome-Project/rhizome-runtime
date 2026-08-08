package repoauthority

import (
	"strings"
	"testing"
	"time"
)

func TestBindMutationOperationRecordsCompleteAcceptedEvidence(t *testing.T) {
	store, ctx, lease, now := bindingFixture(t, time.Minute)

	binding, err := BindMutationOperation(MutationOperationBindingInput{
		Context:       ctx,
		LeaseStore:    store,
		MutationPaths: []string{"owned.go"},
		Now:           now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("BindMutationOperation: %v", err)
	}
	if binding.Schema != MutationOperationBindingSchemaVersion {
		t.Fatalf("schema = %q", binding.Schema)
	}
	if !binding.Accepted {
		t.Fatalf("accepted = false, want true")
	}
	if binding.RepoLeaseID != lease.ID || binding.LeaseTerm != lease.Term {
		t.Fatalf("lease binding = %s/%d, want %s/%d", binding.RepoLeaseID, binding.LeaseTerm, lease.ID, lease.Term)
	}
	if binding.PatchQueueItemID != "patchitem-b1-7" {
		t.Fatalf("patch queue item = %q", binding.PatchQueueItemID)
	}
	if binding.OperationID != "op-b1-7-apply" || binding.OperationKind != "repo_patch_apply" {
		t.Fatalf("operation binding = %q/%q", binding.OperationID, binding.OperationKind)
	}
	if binding.TaskID != "task-b1-7" || binding.SessionID != "session-b1-7" || binding.RunID != "run-b1-7" || binding.AgentID != "agent-b1-7" {
		t.Fatalf("unexpected actor refs in binding: %+v", binding)
	}
	if !strings.HasPrefix(binding.ContextDigest, "sha256:") {
		t.Fatalf("context digest = %q, want sha256", binding.ContextDigest)
	}
	if binding.LeaseContextDigest != lease.ContextDigest {
		t.Fatalf("lease context digest = %q, want %q", binding.LeaseContextDigest, lease.ContextDigest)
	}

	evidence := binding.Evidence()
	if evidence["repo_lease_id"] != lease.ID {
		t.Fatalf("evidence repo_lease_id = %#v", evidence["repo_lease_id"])
	}
	if evidence["lease_term"] != lease.Term {
		t.Fatalf("evidence lease_term = %#v", evidence["lease_term"])
	}
	if evidence["patch_queue_item_id"] != "patchitem-b1-7" {
		t.Fatalf("evidence patch_queue_item_id = %#v", evidence["patch_queue_item_id"])
	}
	if evidence["context_digest"] != binding.ContextDigest {
		t.Fatalf("evidence context_digest = %#v, want %q", evidence["context_digest"], binding.ContextDigest)
	}
}

func TestBindMutationOperationRejectsIncompleteBinding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{
			name:   "missing lease term",
			mutate: func(c *Context) { c.Lease.Term = 0 },
			want:   "lease_term is required",
		},
		{
			name:   "missing patch queue item",
			mutate: func(c *Context) { c.PatchQueue.ItemID = "" },
			want:   "patch_queue_item_id is required",
		},
		{
			name:   "missing operation id",
			mutate: func(c *Context) { c.Operation.ID = "" },
			want:   "operation_id is required",
		},
		{
			name:   "missing operation kind",
			mutate: func(c *Context) { c.Operation.Kind = "" },
			want:   "operation_kind is required",
		},
		{
			name:   "non mutation operation kind",
			mutate: func(c *Context) { c.Operation.Kind = "read_only_probe" },
			want:   "not an accepted repo mutation kind",
		},
		{
			name:   "missing capability snapshot",
			mutate: func(c *Context) { c.CapabilitySnapshot.ID = "" },
			want:   "capability_snapshot.id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, ctx, _, now := bindingFixture(t, time.Minute)
			tt.mutate(&ctx)
			_, err := BindMutationOperation(MutationOperationBindingInput{
				Context:    ctx,
				LeaseStore: store,
				Now:        now.Add(time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindMutationOperation error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindMutationOperationRejectsVagueAuthorityLabels(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{
			name:   "vague lease id",
			mutate: func(c *Context) { c.Lease.ID = "repo_authority" },
			want:   "repo_lease_id has vague repo authority label",
		},
		{
			name:   "vague patch queue id",
			mutate: func(c *Context) { c.PatchQueue.QueueID = "patch queue" },
			want:   "patch_queue_id has vague repo authority label",
		},
		{
			name:   "vague patch queue item",
			mutate: func(c *Context) { c.PatchQueue.ItemID = "mutation" },
			want:   "patch_queue_item_id has vague repo authority label",
		},
		{
			name:   "vague operation id",
			mutate: func(c *Context) { c.Operation.ID = "operation" },
			want:   "operation_id has vague repo authority label",
		},
		{
			name:   "vague operation kind",
			mutate: func(c *Context) { c.Operation.Kind = "repo authority" },
			want:   "operation_kind has vague repo authority label",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, ctx, _, now := bindingFixture(t, time.Minute)
			tt.mutate(&ctx)
			_, err := BindMutationOperation(MutationOperationBindingInput{
				Context:    ctx,
				LeaseStore: store,
				Now:        now.Add(time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindMutationOperation error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindMutationOperationRejectsStaleExpiredAndRevokedLease(t *testing.T) {
	t.Run("stale term", func(t *testing.T) {
		store, ctx, lease, now := bindingFixture(t, time.Minute)
		ctx.Lease.Term = lease.Term + 1

		_, err := BindMutationOperation(MutationOperationBindingInput{
			Context:    ctx,
			LeaseStore: store,
			Now:        now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "lease_term mismatch") {
			t.Fatalf("BindMutationOperation stale term error = %v, want term mismatch", err)
		}
	})

	t.Run("expired lease", func(t *testing.T) {
		store, ctx, _, now := bindingFixture(t, time.Minute)

		_, err := BindMutationOperation(MutationOperationBindingInput{
			Context:    ctx,
			LeaseStore: store,
			Now:        now.Add(time.Minute),
		})
		if err == nil || !strings.Contains(err.Error(), "expired") {
			t.Fatalf("BindMutationOperation expired error = %v, want expired", err)
		}
	})

	t.Run("revoked lease", func(t *testing.T) {
		store, ctx, lease, now := bindingFixture(t, time.Hour)
		if _, err := store.RevokeLeasesForHolder(RevokeFileLeasesInput{
			Holder: HolderForLease(lease),
			Now:    now.Add(time.Second),
			Reason: "test revoke",
		}); err != nil {
			t.Fatalf("RevokeLeasesForHolder: %v", err)
		}

		_, err := BindMutationOperation(MutationOperationBindingInput{
			Context:    ctx,
			LeaseStore: store,
			Now:        now.Add(2 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "revoked") {
			t.Fatalf("BindMutationOperation revoked error = %v, want revoked", err)
		}
	})
}

func TestBindMutationOperationRejectsContextLeaseMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{
			name:   "session mismatch",
			mutate: func(c *Context) { c.SessionID = "session-stale" },
			want:   "session mismatch",
		},
		{
			name:   "run mismatch",
			mutate: func(c *Context) { c.RunID = "run-stale" },
			want:   "run mismatch",
		},
		{
			name:   "agent mismatch",
			mutate: func(c *Context) { c.AgentID = "agent-stale" },
			want:   "agent mismatch",
		},
		{
			name:   "capability snapshot mismatch",
			mutate: func(c *Context) { c.CapabilitySnapshot.ID = "cap-stale" },
			want:   "capability snapshot mismatch",
		},
		{
			name: "pathset mismatch",
			mutate: func(c *Context) {
				c.Pathset = []string{"other.go"}
				c.Base.FileHashes = map[string]string{"other.go": "sha256:base-other"}
			},
			want: "pathset mismatch",
		},
		{
			name: "base tree hash mismatch",
			mutate: func(c *Context) {
				c.Base.TreeHash = "sha256:other-tree"
			},
			want: "lease acquisition context digest mismatch",
		},
		{
			name: "base file hash mismatch",
			mutate: func(c *Context) {
				c.Base.FileHashes["owned.go"] = "sha256:other-file"
			},
			want: "lease acquisition context digest mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, ctx, _, now := bindingFixture(t, time.Minute)
			tt.mutate(&ctx)
			_, err := BindMutationOperation(MutationOperationBindingInput{
				Context:    ctx,
				LeaseStore: store,
				Now:        now.Add(time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindMutationOperation error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func bindingFixture(t *testing.T, ttl time.Duration) (*FileLeaseStore, Context, FileLease, time.Time) {
	t.Helper()
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 21, 19, 0, 0, 0, time.UTC)
	preLease := preLeaseContextWithRefs("agent-b1-7", "task-b1-7", "session-b1-7", "run-b1-7", "cap-b1-7", []string{"owned.go"})
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLease,
		TTL:     ttl,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ctx := preLease
	ctx.Lease = LeaseRef{ID: lease.ID, Term: lease.Term}
	ctx.PatchQueue = PatchQueueRef{QueueID: "patchq-b1-7", ItemID: "patchitem-b1-7"}
	ctx.Operation = OperationRef{ID: "op-b1-7-apply", Kind: "repo_patch_apply"}
	return store, ctx, lease, now
}
