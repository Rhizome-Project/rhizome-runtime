package repoauthority

import (
	"strings"
	"testing"
	"time"
)

func TestContextValidPatchOnlyTempRepo(t *testing.T) {
	pathset, err := NormalizePathSet([]string{"internal/repoauthority/context.go", "internal/repoauthority/context_test.go"})
	if err != nil {
		t.Fatalf("normalize pathset: %v", err)
	}

	ctx := validContext()
	ctx.Pathset = pathset
	ctx.Base.FileHashes["internal/repoauthority/context_test.go"] = "sha256:base-test-file"

	if err := ctx.Validate(); err != nil {
		t.Fatalf("valid context rejected: %v", err)
	}
	if ctx.WithDefaults().Schema != SchemaVersion {
		t.Fatalf("default schema = %q", ctx.WithDefaults().Schema)
	}

	digest1, err := ctx.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	digest2, err := ctx.Digest()
	if err != nil {
		t.Fatalf("digest repeat: %v", err)
	}
	if digest1 != digest2 {
		t.Fatalf("digest is not deterministic: %q != %q", digest1, digest2)
	}
	if !strings.HasPrefix(digest1, "sha256:") {
		t.Fatalf("digest missing sha256 prefix: %q", digest1)
	}

	key, err := ctx.Key()
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	if !strings.HasPrefix(key, "repoctx:v1:patch_only_temp_repo:ws-b1-2:task-b1-2:session-b1-2:run-b1-2:agent-b1-2:") {
		t.Fatalf("unexpected key %q", key)
	}
}

func TestContextValidControlledQueueMode(t *testing.T) {
	ctx := validContext()
	ctx.Mode = ModeControlledQueue

	if err := ctx.Validate(); err != nil {
		t.Fatalf("valid controlled queue context rejected: %v", err)
	}
	digest1, err := ctx.Digest()
	if err != nil {
		t.Fatalf("controlled digest: %v", err)
	}
	digest2, err := ctx.Digest()
	if err != nil {
		t.Fatalf("controlled digest repeat: %v", err)
	}
	if digest1 != digest2 || !strings.HasPrefix(digest1, "sha256:") {
		t.Fatalf("controlled digest is not deterministic canonical sha256: %q vs %q", digest1, digest2)
	}
	key, err := ctx.Key()
	if err != nil {
		t.Fatalf("controlled key: %v", err)
	}
	if !strings.HasPrefix(key, "repoctx:v1:repoauthority_controlled_queue:ws-b1-2:task-b1-2:session-b1-2:run-b1-2:agent-b1-2:") {
		t.Fatalf("unexpected controlled key %q", key)
	}
}

func TestContextControlledQueueModeSupportsLeaseAcquisitionAndQueueDigest(t *testing.T) {
	preLease := validContext()
	preLease.Mode = ModeControlledQueue
	preLease.Lease = LeaseRef{}
	preLease.PatchQueue = PatchQueueRef{}
	preLease.Operation = OperationRef{}

	if err := preLease.Validate(); err != nil {
		t.Fatalf("controlled pre-lease context rejected: %v", err)
	}
	store := NewFileLeaseStore()
	now := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	lease, err := store.Acquire(AcquireFileLeaseInput{
		Context: preLease,
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("controlled Acquire: %v", err)
	}

	queueCtx := preLease
	queueCtx.Lease = LeaseRef{ID: lease.ID, Term: lease.Term}
	queueCtx.PatchQueue = PatchQueueRef{QueueID: "patchq-controlled-b1-2", ItemID: "patchitem-controlled-b1-2"}
	queueDigest, err := patchQueueContextDigest(queueCtx)
	if err != nil {
		t.Fatalf("controlled patch queue digest: %v", err)
	}

	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-controlled-b1-2-apply", Kind: "repo_patch_apply"}
	applyDigest, err := patchQueueContextDigest(applyCtx)
	if err != nil {
		t.Fatalf("controlled apply patch queue digest: %v", err)
	}
	if applyDigest != queueDigest {
		t.Fatalf("patch queue digest should ignore operation refs: queue=%q apply=%q", queueDigest, applyDigest)
	}
	if _, err := BindMutationOperation(MutationOperationBindingInput{
		Context:    applyCtx,
		LeaseStore: store,
		Now:        now.Add(time.Second),
	}); err != nil {
		t.Fatalf("controlled BindMutationOperation: %v", err)
	}
}

func TestContextValidationRejectsMissingIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{name: "workspace", mutate: func(c *Context) { c.WorkspaceID = "" }, want: "workspace_id is required"},
		{name: "task", mutate: func(c *Context) { c.TaskID = " " }, want: "task_id is required"},
		{name: "session", mutate: func(c *Context) { c.SessionID = "" }, want: "session_id is required"},
		{name: "run", mutate: func(c *Context) { c.RunID = "" }, want: "run_id is required"},
		{name: "agent", mutate: func(c *Context) { c.AgentID = "" }, want: "agent_id is required"},
		{name: "principal type", mutate: func(c *Context) { c.Principal.Type = "" }, want: "principal.type is required"},
		{name: "principal id", mutate: func(c *Context) { c.Principal.ID = "" }, want: "principal.id is required"},
		{name: "snapshot", mutate: func(c *Context) { c.CapabilitySnapshot.ID = "" }, want: "capability_snapshot.id is required"},
		{name: "repo root", mutate: func(c *Context) { c.RepoRoot = "" }, want: "repo_root is required"},
		{name: "base identity", mutate: func(c *Context) { c.Base.Ref = ""; c.Base.TreeHash = "" }, want: "base.ref or base.tree_hash is required"},
		{name: "pathset", mutate: func(c *Context) { c.Pathset = nil }, want: "pathset is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := validContext()
			tt.mutate(&ctx)
			err := ctx.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestContextValidationRejectsUnsupportedMode(t *testing.T) {
	ctx := validContext()
	ctx.Mode = "isolated_worktree"

	err := ctx.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported repo authority mode") {
		t.Fatalf("Validate error = %v, want unsupported mode", err)
	}
}

func TestContextValidationRejectsUnstableOrDuplicatePathset(t *testing.T) {
	tests := []struct {
		name    string
		pathset []string
		want    string
	}{
		{name: "unsorted", pathset: []string{"b.go", "a.go"}, want: "pathset is not sorted"},
		{name: "duplicate", pathset: []string{"a.go", "a.go"}, want: "duplicate"},
		{name: "unclean", pathset: []string{"a/../b.go"}, want: "not normalized"},
		{name: "backslash", pathset: []string{"a\\b.go"}, want: "not normalized"},
		{name: "traversal", pathset: []string{"../b.go"}, want: "escapes repo root"},
		{name: "absolute", pathset: []string{"/tmp/b.go"}, want: "absolute paths are not allowed"},
		{name: "leading backslash absolute", pathset: []string{"\\tmp\\b.go"}, want: "absolute paths are not allowed"},
		{name: "windows drive absolute", pathset: []string{"C:\\tmp\\b.go"}, want: "absolute paths are not allowed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := validContext()
			ctx.Pathset = tt.pathset
			err := ctx.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestNormalizePathSetSortsCanonicalPaths(t *testing.T) {
	pathset, err := NormalizePathSet([]string{"z/file.go", "a\\file.go", "m/../m/file.go"})
	if err != nil {
		t.Fatalf("NormalizePathSet: %v", err)
	}
	want := []string{"a/file.go", "m/file.go", "z/file.go"}
	if len(pathset) != len(want) {
		t.Fatalf("pathset length = %d, want %d: %#v", len(pathset), len(want), pathset)
	}
	for i := range want {
		if pathset[i] != want[i] {
			t.Fatalf("pathset[%d] = %q, want %q in %#v", i, pathset[i], want[i], pathset)
		}
	}
}

func TestNormalizePathSetRejectsDuplicateAfterNormalization(t *testing.T) {
	_, err := NormalizePathSet([]string{"a/../b.go", "b.go"})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("NormalizePathSet error = %v, want duplicate", err)
	}
}

func TestContextValidationRejectsIncoherentLeasePatchAndOperationRefs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{
			name: "lease id without term",
			mutate: func(c *Context) {
				c.Lease = LeaseRef{ID: "lease-b1-2"}
			},
			want: "lease.term is required",
		},
		{
			name: "lease term without id",
			mutate: func(c *Context) {
				c.Lease = LeaseRef{Term: 7}
			},
			want: "lease.term requires lease.id",
		},
		{
			name: "patch item without queue",
			mutate: func(c *Context) {
				c.Lease = LeaseRef{ID: "lease-b1-2", Term: 7}
				c.PatchQueue = PatchQueueRef{ItemID: "patch-b1-2"}
			},
			want: "patch_queue.queue_id is required",
		},
		{
			name: "patch item without lease",
			mutate: func(c *Context) {
				c.Lease = LeaseRef{}
				c.PatchQueue = PatchQueueRef{QueueID: "queue-b1-2", ItemID: "patch-b1-2"}
			},
			want: "lease.id is required",
		},
		{
			name: "operation kind without id",
			mutate: func(c *Context) {
				c.Operation = OperationRef{Kind: "repo_patch_apply"}
			},
			want: "operation.kind requires operation.id",
		},
		{
			name: "operation id without kind",
			mutate: func(c *Context) {
				c.Operation = OperationRef{ID: "op-b1-2"}
			},
			want: "operation.kind is required",
		},
		{
			name: "operation without patch item",
			mutate: func(c *Context) {
				c.Lease = LeaseRef{ID: "lease-b1-2", Term: 7}
				c.PatchQueue = PatchQueueRef{}
				c.Operation = OperationRef{ID: "op-b1-2", Kind: "repo_patch_apply"}
			},
			want: "patch_queue.item_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := validContext()
			tt.mutate(&ctx)
			err := ctx.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestContextValidationAllowsMissingBaseHashesAsAddCandidates(t *testing.T) {
	ctx := validContext()
	ctx.Base.FileHashes = nil
	if err := ctx.Validate(); err != nil {
		t.Fatalf("missing base hashes should be allowed as add candidates until CAS proves file state, got %v", err)
	}

	ctx = validContext()
	ctx.Pathset = []string{"internal/repoauthority/context.go", "internal/repoauthority/context_test.go"}
	if err := ctx.Validate(); err != nil {
		t.Fatalf("partial base hashes should be allowed until CAS proves file state, got %v", err)
	}
}

func TestContextValidationValidatesSuppliedBaseHashes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Context)
		want   string
	}{
		{
			name: "empty hash",
			mutate: func(c *Context) {
				c.Base.FileHashes["internal/repoauthority/context.go"] = " "
			},
			want: "base.file_hashes[\"internal/repoauthority/context.go\"] is required",
		},
		{
			name: "unclean hash path",
			mutate: func(c *Context) {
				c.Base.FileHashes["internal/repoauthority/../repoauthority/context.go"] = "sha256:unclean"
			},
			want: "base.file_hashes key is not normalized",
		},
		{
			name: "hash outside pathset",
			mutate: func(c *Context) {
				c.Base.FileHashes["other/file.go"] = "sha256:other"
			},
			want: "base.file_hashes contains path outside pathset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := validContext()
			tt.mutate(&ctx)
			err := ctx.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestContextValidationAllowsConcreteBaseHashesUnderScopedPathset(t *testing.T) {
	ctx := validContext()
	ctx.Pathset = []string{"web/**"}
	ctx.Base.FileHashes = map[string]string{}
	if err := ctx.Validate(); err != nil {
		t.Fatalf("scoped pathset with empty base hashes rejected: %v", err)
	}

	ctx.Base.FileHashes = map[string]string{
		"web/app.js": "sha256:base-web-app",
	}
	if err := ctx.Validate(); err != nil {
		t.Fatalf("scoped pathset with concrete base hash rejected: %v", err)
	}

	ctx.Base.FileHashes["api/server.go"] = "sha256:base-api"
	err := ctx.Validate()
	if err == nil || !strings.Contains(err.Error(), "outside pathset") {
		t.Fatalf("expected base hash outside scoped pathset to fail, got %v", err)
	}
}

func TestPathsetCoversRootGlobEntries(t *testing.T) {
	pathset, err := NormalizePathSet([]string{"index.html", "package.json", "src/app/**", "src/ui/**", "tsconfig*.json", "vite.config.*"})
	if err != nil {
		t.Fatalf("NormalizePathSet: %v", err)
	}
	for _, candidate := range []string{
		"index.html",
		"package.json",
		"tsconfig.json",
		"tsconfig.app.json",
		"vite.config.ts",
		"src/app/main.tsx",
		"src/ui/styles.css",
	} {
		if !pathsetCoversPath(pathset, candidate) {
			t.Fatalf("expected pathset to cover %s in %#v", candidate, pathset)
		}
	}
	for _, candidate := range []string{"src/main.tsx", "src/tsconfig.json", "dist/assets/index.js"} {
		if pathsetCoversPath(pathset, candidate) {
			t.Fatalf("expected pathset not to cover %s in %#v", candidate, pathset)
		}
	}

	ctx := validContext()
	ctx.Pathset = []string{"tsconfig*.json", "vite.config.*"}
	ctx.Base.FileHashes = map[string]string{}
	if err := ctx.Validate(); err != nil {
		t.Fatalf("glob-scoped pathset with empty base hashes rejected: %v", err)
	}
}

func validContext() Context {
	return Context{
		Mode:        ModePatchOnlyTempRepo,
		WorkspaceID: "ws-b1-2",
		TaskID:      "task-b1-2",
		SessionID:   "session-b1-2",
		RunID:       "run-b1-2",
		AgentID:     "agent-b1-2",
		Principal: PrincipalRef{
			Type: "agent",
			ID:   "agent-b1-2",
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     "cap-b1-2",
			Schema: "runtime_capability_snapshot.v1",
		},
		RepoRoot: "C:/work/rhizome",
		Base: BaseIdentity{
			Ref:      "base-b1-2",
			TreeHash: "sha256:base-tree",
			FileHashes: map[string]string{
				"internal/repoauthority/context.go": "sha256:base-file",
			},
		},
		Pathset: []string{"internal/repoauthority/context.go"},
		Lease: LeaseRef{
			ID:   "lease-b1-2",
			Term: 7,
		},
		PatchQueue: PatchQueueRef{
			QueueID: "queue-b1-2",
			ItemID:  "patch-b1-2",
		},
		Operation: OperationRef{
			ID:   "op-b1-2",
			Kind: "repo_patch_apply",
		},
	}
}
