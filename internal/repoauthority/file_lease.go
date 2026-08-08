package repoauthority

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	FileLeaseSchemaVersion = "repo_file_lease.v1"

	FileLeaseStatusActive   = "active"
	FileLeaseStatusExpired  = "expired"
	FileLeaseStatusReleased = "released"
	FileLeaseStatusRevoked  = "revoked"
)

type FileLease struct {
	Schema               string       `json:"schema"`
	ID                   string       `json:"id"`
	Term                 int64        `json:"term"`
	Status               string       `json:"status"`
	WorkspaceID          string       `json:"workspace_id"`
	TaskID               string       `json:"task_id"`
	SessionID            string       `json:"session_id"`
	RunID                string       `json:"run_id"`
	AgentID              string       `json:"agent_id"`
	Principal            PrincipalRef `json:"principal"`
	CapabilitySnapshotID string       `json:"capability_snapshot_id"`
	RepoRoot             string       `json:"repo_root"`
	Pathset              []string     `json:"pathset"`
	ContextDigest        string       `json:"context_digest"`
	AcquiredAt           string       `json:"acquired_at"`
	UpdatedAt            string       `json:"updated_at"`
	ExpiresAt            string       `json:"expires_at"`
}

type FileLeaseHolder struct {
	WorkspaceID          string       `json:"workspace_id"`
	TaskID               string       `json:"task_id"`
	SessionID            string       `json:"session_id"`
	RunID                string       `json:"run_id"`
	AgentID              string       `json:"agent_id"`
	Principal            PrincipalRef `json:"principal"`
	CapabilitySnapshotID string       `json:"capability_snapshot_id"`
	ContextDigest        string       `json:"context_digest"`
}

type AcquireFileLeaseInput struct {
	Context Context
	TTL     time.Duration
	Now     time.Time
}

type RenewFileLeaseInput struct {
	LeaseID string
	Term    int64
	Holder  FileLeaseHolder
	TTL     time.Duration
	Now     time.Time
}

type ReleaseFileLeaseInput struct {
	LeaseID string
	Term    int64
	Holder  FileLeaseHolder
	Now     time.Time
}

type RevokeFileLeasesInput struct {
	Holder FileLeaseHolder
	Now    time.Time
	Reason string
}

type RevokeFileLeasesResult struct {
	Revoked int         `json:"revoked"`
	Leases  []FileLease `json:"leases,omitempty"`
}

type AuthorizeFileMutationInput struct {
	LeaseID string
	Term    int64
	Holder  FileLeaseHolder
	Paths   []string
	Now     time.Time
}

type FileLeaseStore struct {
	mu          sync.Mutex
	nextTerm    int64
	leases      map[string]FileLease
	pathHolders map[string]string
}

func NewFileLeaseStore() *FileLeaseStore {
	return &FileLeaseStore{
		nextTerm:    1,
		leases:      make(map[string]FileLease),
		pathHolders: make(map[string]string),
	}
}

func (s *FileLeaseStore) Acquire(input AcquireFileLeaseInput) (FileLease, error) {
	if s == nil {
		return FileLease{}, fmt.Errorf("file lease store is required")
	}
	if input.TTL <= 0 {
		return FileLease{}, fmt.Errorf("lease ttl must be positive")
	}
	now := normalizeLeaseTime(input.Now)
	authority := input.Context.WithDefaults()
	if err := authority.Validate(); err != nil {
		return FileLease{}, fmt.Errorf("repo authority context: %w", err)
	}
	if strings.TrimSpace(authority.Lease.ID) != "" || strings.TrimSpace(authority.PatchQueue.ItemID) != "" || strings.TrimSpace(authority.Operation.ID) != "" {
		return FileLease{}, fmt.Errorf("lease acquisition context must not already attach lease, patch queue item, or operation")
	}
	digest, err := authority.Digest()
	if err != nil {
		return FileLease{}, err
	}
	pathset, err := NormalizePathSet(authority.Pathset)
	if err != nil {
		return FileLease{}, err
	}
	if len(pathset) == 0 {
		return FileLease{}, fmt.Errorf("pathset is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
	for _, p := range pathset {
		if holderID := s.pathHolders[p]; holderID != "" {
			holder := s.leases[holderID]
			return FileLease{}, fmt.Errorf("path %q is already leased by %s term %d", p, holder.ID, holder.Term)
		}
	}

	term := s.nextTerm
	s.nextTerm++
	lease := FileLease{
		Schema:               FileLeaseSchemaVersion,
		ID:                   buildFileLeaseID(authority, term, digest),
		Term:                 term,
		Status:               FileLeaseStatusActive,
		WorkspaceID:          authority.WorkspaceID,
		TaskID:               authority.TaskID,
		SessionID:            authority.SessionID,
		RunID:                authority.RunID,
		AgentID:              authority.AgentID,
		Principal:            authority.Principal,
		CapabilitySnapshotID: authority.CapabilitySnapshot.ID,
		RepoRoot:             authority.RepoRoot,
		Pathset:              append([]string(nil), pathset...),
		ContextDigest:        digest,
		AcquiredAt:           formatLeaseTime(now),
		UpdatedAt:            formatLeaseTime(now),
		ExpiresAt:            formatLeaseTime(now.Add(input.TTL)),
	}
	s.leases[lease.ID] = cloneFileLease(lease)
	for _, p := range lease.Pathset {
		s.pathHolders[p] = lease.ID
	}
	return cloneFileLease(lease), nil
}

func (s *FileLeaseStore) Renew(input RenewFileLeaseInput) (FileLease, error) {
	if s == nil {
		return FileLease{}, fmt.Errorf("file lease store is required")
	}
	if input.TTL <= 0 {
		return FileLease{}, fmt.Errorf("lease ttl must be positive")
	}
	now := normalizeLeaseTime(input.Now)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)

	lease, err := s.requireLiveLeaseLocked(input.LeaseID, input.Term, input.Holder, now)
	if err != nil {
		return FileLease{}, err
	}
	lease.UpdatedAt = formatLeaseTime(now)
	lease.ExpiresAt = formatLeaseTime(now.Add(input.TTL))
	s.leases[lease.ID] = cloneFileLease(lease)
	return cloneFileLease(lease), nil
}

func (s *FileLeaseStore) Release(input ReleaseFileLeaseInput) (FileLease, error) {
	if s == nil {
		return FileLease{}, fmt.Errorf("file lease store is required")
	}
	now := normalizeLeaseTime(input.Now)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)

	lease, err := s.requireLiveLeaseLocked(input.LeaseID, input.Term, input.Holder, now)
	if err != nil {
		return FileLease{}, err
	}
	lease.Status = FileLeaseStatusReleased
	lease.UpdatedAt = formatLeaseTime(now)
	s.leases[lease.ID] = cloneFileLease(lease)
	for _, p := range lease.Pathset {
		if s.pathHolders[p] == lease.ID {
			delete(s.pathHolders, p)
		}
	}
	return cloneFileLease(lease), nil
}

func (s *FileLeaseStore) RevokeLeasesForHolder(input RevokeFileLeasesInput) (RevokeFileLeasesResult, error) {
	if s == nil {
		return RevokeFileLeasesResult{}, fmt.Errorf("file lease store is required")
	}
	if err := validateFileLeaseHolder(input.Holder); err != nil {
		return RevokeFileLeasesResult{}, err
	}
	now := normalizeLeaseTime(input.Now)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)

	result := RevokeFileLeasesResult{Leases: make([]FileLease, 0, 1)}
	for id, lease := range s.leases {
		if lease.Status != FileLeaseStatusActive {
			continue
		}
		if verifyFileLeaseHolder(lease, input.Holder) != nil {
			continue
		}
		lease.Status = FileLeaseStatusRevoked
		lease.UpdatedAt = formatLeaseTime(now)
		s.leases[id] = lease
		for _, p := range lease.Pathset {
			if s.pathHolders[p] == id {
				delete(s.pathHolders, p)
			}
		}
		result.Revoked++
		result.Leases = append(result.Leases, cloneFileLease(lease))
	}
	return result, nil
}

func (s *FileLeaseStore) AuthorizeMutation(input AuthorizeFileMutationInput) error {
	if s == nil {
		return fmt.Errorf("file lease store is required")
	}
	now := normalizeLeaseTime(input.Now)
	paths, err := NormalizePathSet(input.Paths)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("mutation paths are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
	lease, err := s.requireLiveLeaseLocked(input.LeaseID, input.Term, input.Holder, now)
	if err != nil {
		return err
	}
	leased := make(map[string]struct{}, len(lease.Pathset))
	for _, p := range lease.Pathset {
		leased[p] = struct{}{}
	}
	for _, p := range paths {
		if _, ok := leased[p]; !ok {
			return fmt.Errorf("path %q is not covered by lease %s", p, lease.ID)
		}
	}
	return nil
}

func (s *FileLeaseStore) Snapshot() []FileLease {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FileLease, 0, len(s.leases))
	for _, lease := range s.leases {
		out = append(out, cloneFileLease(lease))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Term != out[j].Term {
			return out[i].Term < out[j].Term
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (s *FileLeaseStore) requireLiveLeaseLocked(leaseID string, term int64, holder FileLeaseHolder, now time.Time) (FileLease, error) {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		return FileLease{}, fmt.Errorf("lease.id is required")
	}
	if term <= 0 {
		return FileLease{}, fmt.Errorf("lease.term is required")
	}
	if err := validateFileLeaseHolder(holder); err != nil {
		return FileLease{}, err
	}
	lease, ok := s.leases[leaseID]
	if !ok {
		return FileLease{}, fmt.Errorf("lease %q not found", leaseID)
	}
	if lease.Status != FileLeaseStatusActive {
		return FileLease{}, fmt.Errorf("lease %s is %s", lease.ID, lease.Status)
	}
	if lease.Term != term {
		return FileLease{}, fmt.Errorf("lease.term mismatch: got %d want %d", term, lease.Term)
	}
	if leaseExpiredAt(lease, now) {
		lease.Status = FileLeaseStatusExpired
		lease.UpdatedAt = formatLeaseTime(now)
		s.leases[lease.ID] = lease
		for _, p := range lease.Pathset {
			if s.pathHolders[p] == lease.ID {
				delete(s.pathHolders, p)
			}
		}
		return FileLease{}, fmt.Errorf("lease %s is expired", lease.ID)
	}
	if err := verifyFileLeaseHolder(lease, holder); err != nil {
		return FileLease{}, err
	}
	return lease, nil
}

func HolderForLease(lease FileLease) FileLeaseHolder {
	return FileLeaseHolder{
		WorkspaceID:          lease.WorkspaceID,
		TaskID:               lease.TaskID,
		SessionID:            lease.SessionID,
		RunID:                lease.RunID,
		AgentID:              lease.AgentID,
		Principal:            lease.Principal,
		CapabilitySnapshotID: lease.CapabilitySnapshotID,
		ContextDigest:        lease.ContextDigest,
	}
}

func validateFileLeaseHolder(holder FileLeaseHolder) error {
	if err := requireNonEmpty("holder.workspace_id", holder.WorkspaceID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.task_id", holder.TaskID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.session_id", holder.SessionID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.run_id", holder.RunID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.agent_id", holder.AgentID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.principal.type", holder.Principal.Type); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.principal.id", holder.Principal.ID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.capability_snapshot_id", holder.CapabilitySnapshotID); err != nil {
		return err
	}
	if err := requireNonEmpty("holder.context_digest", holder.ContextDigest); err != nil {
		return err
	}
	return nil
}

func verifyFileLeaseHolder(lease FileLease, holder FileLeaseHolder) error {
	switch {
	case strings.TrimSpace(holder.WorkspaceID) != lease.WorkspaceID:
		return fmt.Errorf("lease holder workspace mismatch: got %q want %q", holder.WorkspaceID, lease.WorkspaceID)
	case strings.TrimSpace(holder.TaskID) != lease.TaskID:
		return fmt.Errorf("lease holder task mismatch: got %q want %q", holder.TaskID, lease.TaskID)
	case strings.TrimSpace(holder.SessionID) != lease.SessionID:
		return fmt.Errorf("lease holder session mismatch: got %q want %q", holder.SessionID, lease.SessionID)
	case strings.TrimSpace(holder.RunID) != lease.RunID:
		return fmt.Errorf("lease holder run mismatch: got %q want %q", holder.RunID, lease.RunID)
	case strings.TrimSpace(holder.AgentID) != lease.AgentID:
		return fmt.Errorf("lease holder agent mismatch: got %q want %q", holder.AgentID, lease.AgentID)
	case strings.TrimSpace(holder.Principal.Type) != lease.Principal.Type || strings.TrimSpace(holder.Principal.ID) != lease.Principal.ID:
		return fmt.Errorf("lease principal mismatch")
	case strings.TrimSpace(holder.CapabilitySnapshotID) != lease.CapabilitySnapshotID:
		return fmt.Errorf("lease holder capability snapshot mismatch: got %q want %q", holder.CapabilitySnapshotID, lease.CapabilitySnapshotID)
	case strings.TrimSpace(holder.ContextDigest) != lease.ContextDigest:
		return fmt.Errorf("lease holder context digest mismatch")
	default:
		return nil
	}
}

func (s *FileLeaseStore) expireLocked(now time.Time) {
	for id, lease := range s.leases {
		if lease.Status != FileLeaseStatusActive || !leaseExpiredAt(lease, now) {
			continue
		}
		lease.Status = FileLeaseStatusExpired
		lease.UpdatedAt = formatLeaseTime(now)
		s.leases[id] = lease
		for _, p := range lease.Pathset {
			if s.pathHolders[p] == id {
				delete(s.pathHolders, p)
			}
		}
	}
}

func leaseExpiredAt(lease FileLease, now time.Time) bool {
	expiresAt, err := time.Parse(time.RFC3339Nano, lease.ExpiresAt)
	if err != nil {
		return true
	}
	return !now.Before(expiresAt)
}

func buildFileLeaseID(authority Context, term int64, digest string) string {
	shortDigest := strings.TrimPrefix(digest, "sha256:")
	if len(shortDigest) > 12 {
		shortDigest = shortDigest[:12]
	}
	parts := []string{
		"repolease",
		"v1",
		cleanKeyPart(authority.WorkspaceID),
		cleanKeyPart(authority.TaskID),
		cleanKeyPart(authority.AgentID),
		fmt.Sprintf("t%d", term),
		shortDigest,
	}
	return strings.Join(parts, ":")
}

func cloneFileLease(lease FileLease) FileLease {
	lease.Pathset = append([]string(nil), lease.Pathset...)
	return lease
}

func normalizeLeaseTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

func formatLeaseTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
