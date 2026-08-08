package repoauthority

import (
	"fmt"
	"strings"
	"time"
)

const MutationOperationBindingSchemaVersion = "repo_mutation_operation_binding.v1"

var allowedMutationOperationKinds = map[string]struct{}{
	"repo_patch_apply": {},
}

type MutationOperationBindingInput struct {
	Context       Context
	LeaseStore    *FileLeaseStore
	MutationPaths []string
	Now           time.Time
}

type MutationOperationBinding struct {
	Schema                   string   `json:"schema"`
	Accepted                 bool     `json:"accepted"`
	RepoLeaseID              string   `json:"repo_lease_id"`
	LeaseTerm                int64    `json:"lease_term"`
	PatchQueueID             string   `json:"patch_queue_id"`
	PatchQueueItemID         string   `json:"patch_queue_item_id"`
	OperationID              string   `json:"operation_id"`
	OperationKind            string   `json:"operation_kind"`
	WorkspaceID              string   `json:"workspace_id"`
	TaskID                   string   `json:"task_id"`
	SessionID                string   `json:"session_id"`
	RunID                    string   `json:"run_id"`
	AgentID                  string   `json:"agent_id"`
	PrincipalType            string   `json:"principal_type"`
	PrincipalID              string   `json:"principal_id"`
	CapabilitySnapshotID     string   `json:"capability_snapshot_id"`
	CapabilitySnapshotSchema string   `json:"capability_snapshot_schema,omitempty"`
	BaseRef                  string   `json:"base_ref,omitempty"`
	BaseTreeHash             string   `json:"base_tree_hash,omitempty"`
	ContextDigest            string   `json:"context_digest"`
	LeaseContextDigest       string   `json:"lease_context_digest"`
	MutationPaths            []string `json:"mutation_paths"`
}

func BindMutationOperation(input MutationOperationBindingInput) (MutationOperationBinding, error) {
	if input.LeaseStore == nil {
		return MutationOperationBinding{}, fmt.Errorf("file lease store is required")
	}
	authority := input.Context.WithDefaults()
	if err := requireConcreteMutationOperationRefs(authority); err != nil {
		return MutationOperationBinding{}, err
	}
	contextDigest, err := authority.Digest()
	if err != nil {
		return MutationOperationBinding{}, fmt.Errorf("repo authority context: %w", err)
	}
	lease, err := findBindingLease(input.LeaseStore, authority.Lease.ID)
	if err != nil {
		return MutationOperationBinding{}, err
	}
	if err := verifyMutationBindingContext(authority, lease); err != nil {
		return MutationOperationBinding{}, err
	}
	if err := verifyLeaseAcquisitionContextDigest(authority, lease); err != nil {
		return MutationOperationBinding{}, err
	}

	mutationPaths := input.MutationPaths
	if len(mutationPaths) == 0 {
		mutationPaths = authority.Pathset
	}
	normalizedPaths, err := NormalizePathSet(mutationPaths)
	if err != nil {
		return MutationOperationBinding{}, fmt.Errorf("mutation paths: %w", err)
	}
	if len(normalizedPaths) == 0 {
		return MutationOperationBinding{}, fmt.Errorf("mutation paths are required")
	}
	if err := input.LeaseStore.AuthorizeMutation(AuthorizeFileMutationInput{
		LeaseID: authority.Lease.ID,
		Term:    authority.Lease.Term,
		Holder:  HolderForLease(lease),
		Paths:   normalizedPaths,
		Now:     input.Now,
	}); err != nil {
		return MutationOperationBinding{}, fmt.Errorf("mutation operation lease authorization: %w", err)
	}

	return MutationOperationBinding{
		Schema:                   MutationOperationBindingSchemaVersion,
		Accepted:                 true,
		RepoLeaseID:              authority.Lease.ID,
		LeaseTerm:                authority.Lease.Term,
		PatchQueueID:             authority.PatchQueue.QueueID,
		PatchQueueItemID:         authority.PatchQueue.ItemID,
		OperationID:              authority.Operation.ID,
		OperationKind:            authority.Operation.Kind,
		WorkspaceID:              authority.WorkspaceID,
		TaskID:                   authority.TaskID,
		SessionID:                authority.SessionID,
		RunID:                    authority.RunID,
		AgentID:                  authority.AgentID,
		PrincipalType:            authority.Principal.Type,
		PrincipalID:              authority.Principal.ID,
		CapabilitySnapshotID:     authority.CapabilitySnapshot.ID,
		CapabilitySnapshotSchema: authority.CapabilitySnapshot.Schema,
		BaseRef:                  authority.Base.Ref,
		BaseTreeHash:             authority.Base.TreeHash,
		ContextDigest:            contextDigest,
		LeaseContextDigest:       lease.ContextDigest,
		MutationPaths:            append([]string(nil), normalizedPaths...),
	}, nil
}

func ValidateConcreteMutationOperationRefs(authority Context) error {
	return requireConcreteMutationOperationRefs(authority.WithDefaults())
}

func (b MutationOperationBinding) Evidence() map[string]any {
	return map[string]any{
		"schema":                     b.Schema,
		"accepted":                   b.Accepted,
		"repo_lease_id":              b.RepoLeaseID,
		"lease_term":                 b.LeaseTerm,
		"patch_queue_id":             b.PatchQueueID,
		"patch_queue_item_id":        b.PatchQueueItemID,
		"operation_id":               b.OperationID,
		"operation_kind":             b.OperationKind,
		"workspace_id":               b.WorkspaceID,
		"task_id":                    b.TaskID,
		"session_id":                 b.SessionID,
		"run_id":                     b.RunID,
		"agent_id":                   b.AgentID,
		"principal_type":             b.PrincipalType,
		"principal_id":               b.PrincipalID,
		"capability_snapshot_id":     b.CapabilitySnapshotID,
		"capability_snapshot_schema": b.CapabilitySnapshotSchema,
		"base_ref":                   b.BaseRef,
		"base_tree_hash":             b.BaseTreeHash,
		"context_digest":             b.ContextDigest,
		"lease_context_digest":       b.LeaseContextDigest,
		"mutation_paths":             append([]string(nil), b.MutationPaths...),
	}
}

func requireConcreteMutationOperationRefs(authority Context) error {
	if err := rejectVagueBindingLabel("repo_lease_id", authority.Lease.ID); err != nil {
		return err
	}
	if authority.Lease.Term <= 0 {
		return fmt.Errorf("lease_term is required")
	}
	if err := rejectVagueBindingLabel("patch_queue_id", authority.PatchQueue.QueueID); err != nil {
		return err
	}
	if err := rejectVagueBindingLabel("patch_queue_item_id", authority.PatchQueue.ItemID); err != nil {
		return err
	}
	if err := rejectVagueBindingLabel("operation_id", authority.Operation.ID); err != nil {
		return err
	}
	if err := rejectVagueBindingLabel("operation_kind", authority.Operation.Kind); err != nil {
		return err
	}
	if _, ok := allowedMutationOperationKinds[strings.TrimSpace(authority.Operation.Kind)]; !ok {
		return fmt.Errorf("operation_kind %q is not an accepted repo mutation kind", authority.Operation.Kind)
	}
	return nil
}

func rejectVagueBindingLabel(field, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s is required", field)
	}
	if isVagueRepoAuthorityLabel(trimmed) {
		return fmt.Errorf("%s has vague repo authority label %q", field, value)
	}
	return nil
}

func isVagueRepoAuthorityLabel(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(" ", "", "_", "", "-", "", ".", "", ":", "", "/", "", "\\", "")
	normalized = replacer.Replace(normalized)
	switch normalized {
	case "",
		"repo",
		"authority",
		"repoauthority",
		"lease",
		"repolease",
		"patch",
		"queue",
		"patchqueue",
		"operation",
		"mutation",
		"write",
		"file",
		"default",
		"generic",
		"unknown",
		"todo",
		"placeholder",
		"temp",
		"temporary",
		"none",
		"na",
		"all":
		return true
	default:
		return false
	}
}

func findBindingLease(store *FileLeaseStore, leaseID string) (FileLease, error) {
	leaseID = strings.TrimSpace(leaseID)
	for _, lease := range store.Snapshot() {
		if lease.ID == leaseID {
			return lease, nil
		}
	}
	return FileLease{}, fmt.Errorf("repo_lease_id %q not found", leaseID)
}

func verifyMutationBindingContext(authority Context, lease FileLease) error {
	switch {
	case authority.Lease.Term != lease.Term:
		return fmt.Errorf("lease_term mismatch: got %d want %d", authority.Lease.Term, lease.Term)
	case strings.TrimSpace(authority.WorkspaceID) != lease.WorkspaceID:
		return fmt.Errorf("lease context workspace mismatch: got %q want %q", authority.WorkspaceID, lease.WorkspaceID)
	case strings.TrimSpace(authority.TaskID) != lease.TaskID:
		return fmt.Errorf("lease context task mismatch: got %q want %q", authority.TaskID, lease.TaskID)
	case strings.TrimSpace(authority.SessionID) != lease.SessionID:
		return fmt.Errorf("lease context session mismatch: got %q want %q", authority.SessionID, lease.SessionID)
	case strings.TrimSpace(authority.RunID) != lease.RunID:
		return fmt.Errorf("lease context run mismatch: got %q want %q", authority.RunID, lease.RunID)
	case strings.TrimSpace(authority.AgentID) != lease.AgentID:
		return fmt.Errorf("lease context agent mismatch: got %q want %q", authority.AgentID, lease.AgentID)
	case strings.TrimSpace(authority.Principal.Type) != lease.Principal.Type || strings.TrimSpace(authority.Principal.ID) != lease.Principal.ID:
		return fmt.Errorf("lease context principal mismatch")
	case strings.TrimSpace(authority.CapabilitySnapshot.ID) != lease.CapabilitySnapshotID:
		return fmt.Errorf("lease context capability snapshot mismatch: got %q want %q", authority.CapabilitySnapshot.ID, lease.CapabilitySnapshotID)
	case strings.TrimSpace(authority.RepoRoot) != lease.RepoRoot:
		return fmt.Errorf("lease context repo root mismatch: got %q want %q", authority.RepoRoot, lease.RepoRoot)
	}
	pathset, err := NormalizePathSet(authority.Pathset)
	if err != nil {
		return fmt.Errorf("lease context pathset: %w", err)
	}
	if !sameStringSlice(pathset, lease.Pathset) {
		return fmt.Errorf("lease context pathset mismatch: got %#v want %#v", pathset, lease.Pathset)
	}
	return nil
}

func verifyLeaseAcquisitionContextDigest(authority Context, lease FileLease) error {
	preLease := authority
	preLease.Lease = LeaseRef{}
	preLease.PatchQueue = PatchQueueRef{}
	preLease.Operation = OperationRef{}
	digest, err := preLease.Digest()
	if err != nil {
		return fmt.Errorf("lease acquisition context digest: %w", err)
	}
	if digest != lease.ContextDigest {
		return fmt.Errorf("lease acquisition context digest mismatch: got %q want %q", digest, lease.ContextDigest)
	}
	return nil
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
