package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

const (
	SchemaVersion         = "repo_authority_context.v1"
	ModePatchOnlyTempRepo = "patch_only_temp_repo"
	ModeControlledQueue   = "repoauthority_controlled_queue"
)

type Context struct {
	Schema             string                `json:"schema"`
	Mode               string                `json:"mode"`
	WorkspaceID        string                `json:"workspace_id"`
	TaskID             string                `json:"task_id"`
	SessionID          string                `json:"session_id"`
	RunID              string                `json:"run_id"`
	AgentID            string                `json:"agent_id"`
	Principal          PrincipalRef          `json:"principal"`
	CapabilitySnapshot CapabilitySnapshotRef `json:"capability_snapshot"`
	RepoRoot           string                `json:"repo_root"`
	Base               BaseIdentity          `json:"base,omitempty"`
	Pathset            []string              `json:"pathset,omitempty"`
	Lease              LeaseRef              `json:"lease,omitempty"`
	PatchQueue         PatchQueueRef         `json:"patch_queue,omitempty"`
	Operation          OperationRef          `json:"operation,omitempty"`
}

type PrincipalRef struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type CapabilitySnapshotRef struct {
	ID     string `json:"id"`
	Schema string `json:"schema,omitempty"`
}

type BaseIdentity struct {
	Ref        string            `json:"ref,omitempty"`
	TreeHash   string            `json:"tree_hash,omitempty"`
	FileHashes map[string]string `json:"file_hashes,omitempty"`
}

type LeaseRef struct {
	ID   string `json:"id,omitempty"`
	Term int64  `json:"term,omitempty"`
}

type PatchQueueRef struct {
	QueueID string `json:"queue_id,omitempty"`
	ItemID  string `json:"item_id,omitempty"`
}

type OperationRef struct {
	ID   string `json:"id,omitempty"`
	Kind string `json:"kind,omitempty"`
}

func (c Context) Validate() error {
	if strings.TrimSpace(c.Schema) != "" && strings.TrimSpace(c.Schema) != SchemaVersion {
		return fmt.Errorf("unsupported repo authority context schema %q", c.Schema)
	}
	switch strings.TrimSpace(c.Mode) {
	case ModePatchOnlyTempRepo, ModeControlledQueue:
	default:
		return fmt.Errorf("unsupported repo authority mode %q", c.Mode)
	}
	if err := requireNonEmpty("workspace_id", c.WorkspaceID); err != nil {
		return err
	}
	if err := requireNonEmpty("task_id", c.TaskID); err != nil {
		return err
	}
	if err := requireNonEmpty("session_id", c.SessionID); err != nil {
		return err
	}
	if err := requireNonEmpty("run_id", c.RunID); err != nil {
		return err
	}
	if err := requireNonEmpty("agent_id", c.AgentID); err != nil {
		return err
	}
	if err := requireNonEmpty("principal.type", c.Principal.Type); err != nil {
		return err
	}
	if err := requireNonEmpty("principal.id", c.Principal.ID); err != nil {
		return err
	}
	if err := requireNonEmpty("capability_snapshot.id", c.CapabilitySnapshot.ID); err != nil {
		return err
	}
	if err := requireNonEmpty("repo_root", c.RepoRoot); err != nil {
		return err
	}
	if err := validateCanonicalPathset(c.Pathset); err != nil {
		return err
	}
	if err := validateBaseIdentity(c.Base, c.Pathset); err != nil {
		return err
	}
	if err := validateLeaseRef(c.Lease); err != nil {
		return err
	}
	if err := validatePatchQueueRef(c.PatchQueue, c.Lease); err != nil {
		return err
	}
	if err := validateOperationRef(c.Operation, c.Lease, c.PatchQueue); err != nil {
		return err
	}
	return nil
}

func (c Context) WithDefaults() Context {
	if strings.TrimSpace(c.Schema) == "" {
		c.Schema = SchemaVersion
	}
	return c
}

func (c Context) Digest() (string, error) {
	c = c.WithDefaults()
	if err := c.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("marshal repo authority context: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (c Context) Key() (string, error) {
	digest, err := c.Digest()
	if err != nil {
		return "", err
	}
	shortDigest := strings.TrimPrefix(digest, "sha256:")
	if len(shortDigest) > 16 {
		shortDigest = shortDigest[:16]
	}
	return strings.Join([]string{
		"repoctx",
		"v1",
		cleanKeyPart(c.Mode),
		cleanKeyPart(c.WorkspaceID),
		cleanKeyPart(c.TaskID),
		cleanKeyPart(c.SessionID),
		cleanKeyPart(c.RunID),
		cleanKeyPart(c.AgentID),
		shortDigest,
	}, ":"), nil
}

func NormalizePathSet(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	normalized := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for i, raw := range paths {
		p, err := NormalizePath(raw)
		if err != nil {
			return nil, fmt.Errorf("pathset[%d]: %w", i, err)
		}
		if _, ok := seen[p]; ok {
			return nil, fmt.Errorf("pathset contains duplicate path %q", p)
		}
		seen[p] = struct{}{}
		normalized = append(normalized, p)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func NormalizePath(raw string) (string, error) {
	if strings.ContainsRune(raw, '\x00') {
		return "", fmt.Errorf("path contains NUL byte")
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("path is required")
	}
	if hasWindowsVolume(trimmed) {
		return "", fmt.Errorf("absolute paths are not allowed: %q", raw)
	}
	slashPath := strings.ReplaceAll(trimmed, "\\", "/")
	if strings.HasPrefix(slashPath, "/") || path.IsAbs(slashPath) {
		return "", fmt.Errorf("absolute paths are not allowed: %q", raw)
	}
	cleaned := path.Clean(slashPath)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("path is required")
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes repo root: %q", raw)
	}
	return cleaned, nil
}

func requireNonEmpty(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	return nil
}

func validateCanonicalPathset(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(paths))
	previous := ""
	for i, raw := range paths {
		normalized, err := NormalizePath(raw)
		if err != nil {
			return fmt.Errorf("pathset[%d]: %w", i, err)
		}
		if raw != normalized {
			return fmt.Errorf("pathset[%d] is not normalized: got %q want %q", i, raw, normalized)
		}
		if _, ok := seen[normalized]; ok {
			return fmt.Errorf("pathset contains duplicate path %q", normalized)
		}
		if i > 0 && normalized < previous {
			return fmt.Errorf("pathset is not sorted at index %d: %q before %q", i, normalized, previous)
		}
		seen[normalized] = struct{}{}
		previous = normalized
	}
	return nil
}

func validateBaseIdentity(base BaseIdentity, pathset []string) error {
	if strings.TrimSpace(base.Ref) == "" && strings.TrimSpace(base.TreeHash) == "" {
		return fmt.Errorf("base.ref or base.tree_hash is required")
	}
	if len(pathset) == 0 {
		return fmt.Errorf("pathset is required")
	}
	for rawPath, hash := range base.FileHashes {
		normalized, err := NormalizePath(rawPath)
		if err != nil {
			return fmt.Errorf("base.file_hashes[%q]: %w", rawPath, err)
		}
		if normalized != rawPath {
			return fmt.Errorf("base.file_hashes key is not normalized: got %q want %q", rawPath, normalized)
		}
		if strings.TrimSpace(hash) == "" {
			return fmt.Errorf("base.file_hashes[%q] is required", rawPath)
		}
		if !pathsetCoversPath(pathset, rawPath) {
			return fmt.Errorf("base.file_hashes contains path outside pathset: %q", rawPath)
		}
	}
	return nil
}

func pathsetAllEntriesScoped(paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		if !pathsetEntryIsScoped(path) {
			return false
		}
	}
	return true
}

func pathsetCoversPath(pathset []string, pathValue string) bool {
	for _, scope := range pathset {
		if pathsetEntryCoversPath(scope, pathValue) {
			return true
		}
	}
	return false
}

func pathsetEntryCoversPath(scope, pathValue string) bool {
	scope = strings.TrimSpace(scope)
	pathValue = strings.TrimSpace(pathValue)
	if scope == "" || pathValue == "" {
		return false
	}
	if scope == "*" || scope == "**" {
		return true
	}
	if scope == pathValue {
		return true
	}
	if strings.HasSuffix(scope, "/**") {
		prefix := strings.TrimSuffix(scope, "/**")
		return pathValue == prefix || strings.HasPrefix(pathValue, prefix+"/")
	}
	if strings.HasSuffix(scope, "/*") {
		prefix := strings.TrimSuffix(scope, "/*")
		if pathValue == prefix || !strings.HasPrefix(pathValue, prefix+"/") {
			return false
		}
		return !strings.Contains(strings.TrimPrefix(pathValue, prefix+"/"), "/")
	}
	if pathsetEntryGlobCoversPath(scope, pathValue) {
		return true
	}
	return false
}

func pathsetEntryIsScoped(scope string) bool {
	scope = strings.TrimSpace(scope)
	return scope == "*" || scope == "**" || strings.HasSuffix(scope, "/*") || strings.HasSuffix(scope, "/**") || pathsetEntryIsValidGlob(scope)
}

func pathsetEntryGlobCoversPath(scope, pathValue string) bool {
	if !pathsetEntryHasGlob(scope) {
		return false
	}
	matched, err := path.Match(scope, pathValue)
	return err == nil && matched
}

func pathsetEntryIsValidGlob(scope string) bool {
	if !pathsetEntryHasGlob(scope) {
		return false
	}
	_, err := path.Match(scope, "")
	return err == nil
}

func pathsetEntryHasGlob(scope string) bool {
	return strings.ContainsAny(scope, "*?[")
}

func validateLeaseRef(lease LeaseRef) error {
	id := strings.TrimSpace(lease.ID)
	switch {
	case id == "" && lease.Term != 0:
		return fmt.Errorf("lease.term requires lease.id")
	case id != "" && lease.Term <= 0:
		return fmt.Errorf("lease.term is required when lease.id is present")
	}
	return nil
}

func validatePatchQueueRef(patch PatchQueueRef, lease LeaseRef) error {
	queueID := strings.TrimSpace(patch.QueueID)
	itemID := strings.TrimSpace(patch.ItemID)
	switch {
	case itemID != "" && queueID == "":
		return fmt.Errorf("patch_queue.queue_id is required when patch_queue.item_id is present")
	case itemID != "" && strings.TrimSpace(lease.ID) == "":
		return fmt.Errorf("lease.id is required when patch_queue.item_id is present")
	}
	return nil
}

func validateOperationRef(op OperationRef, lease LeaseRef, patch PatchQueueRef) error {
	opID := strings.TrimSpace(op.ID)
	if opID == "" {
		if strings.TrimSpace(op.Kind) != "" {
			return fmt.Errorf("operation.kind requires operation.id")
		}
		return nil
	}
	if strings.TrimSpace(op.Kind) == "" {
		return fmt.Errorf("operation.kind is required when operation.id is present")
	}
	if strings.TrimSpace(lease.ID) == "" {
		return fmt.Errorf("lease.id is required when operation.id is present")
	}
	if strings.TrimSpace(patch.ItemID) == "" {
		return fmt.Errorf("patch_queue.item_id is required when operation.id is present")
	}
	return nil
}

func cleanKeyPart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, ":", "_")
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "\\", "_")
	if value == "" {
		return "_"
	}
	return value
}

func hasWindowsVolume(value string) bool {
	if len(value) < 2 || value[1] != ':' {
		return false
	}
	first := value[0]
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')
}
