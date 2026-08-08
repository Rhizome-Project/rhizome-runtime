package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

const (
	CASPatchApplySchemaVersion = "repo_cas_patch_apply.v1"

	CASPatchStatusApplied  = "applied"
	CASPatchStatusConflict = "conflict"
	CASPatchStatusFailed   = "failed"

	CASPatchChangeModify = "modify"
	CASPatchChangeAdd    = "add"

	CASPatchIssueContextInvalid          = "context_invalid"
	CASPatchIssueCandidateHashesRequired = "candidate_file_hashes_required"
	CASPatchIssueCandidatePathInvalid    = "candidate_path_invalid"
	CASPatchIssueCandidatePathUnstable   = "candidate_path_unstable"
	CASPatchIssueCandidatePathDuplicate  = "candidate_path_duplicate"
	CASPatchIssueCandidateHashMissing    = "missing_candidate_hash"
	CASPatchIssuePathOutsideContext      = "path_outside_context"
	CASPatchIssueBaseHashMissing         = "missing_base_hash"
	CASPatchIssueCurrentHashMissing      = "missing_current_hash"
	CASPatchIssueBaseDrift               = "base_drift"
)

type CASPatchApplyInput struct {
	Context             Context           `json:"context"`
	PatchID             string            `json:"patch_id,omitempty"`
	CurrentFileHashes   map[string]string `json:"current_file_hashes"`
	CandidateFileHashes map[string]string `json:"candidate_file_hashes"`
}

type CASPatchApplyResult struct {
	Schema        string               `json:"schema"`
	Status        string               `json:"status"`
	PatchID       string               `json:"patch_id,omitempty"`
	PatchDigest   string               `json:"patch_digest,omitempty"`
	ContextDigest string               `json:"context_digest,omitempty"`
	Paths         []CASPatchPathResult `json:"paths,omitempty"`
	Issues        []CASPatchIssue      `json:"issues,omitempty"`
}

type CASPatchPathResult struct {
	Path          string `json:"path"`
	Status        string `json:"status"`
	ChangeKind    string `json:"change_kind,omitempty"`
	BaseHash      string `json:"base_hash,omitempty"`
	CurrentHash   string `json:"current_hash,omitempty"`
	CandidateHash string `json:"candidate_hash,omitempty"`
}

type CASPatchIssue struct {
	Status        string `json:"status"`
	Kind          string `json:"kind"`
	Path          string `json:"path,omitempty"`
	Message       string `json:"message"`
	ExpectedHash  string `json:"expected_hash,omitempty"`
	ActualHash    string `json:"actual_hash,omitempty"`
	CandidateHash string `json:"candidate_hash,omitempty"`
}

type casPatchCandidateEntry struct {
	path string
	hash string
}

func EvaluateCASPatchApply(input CASPatchApplyInput) CASPatchApplyResult {
	result := CASPatchApplyResult{
		Schema:  CASPatchApplySchemaVersion,
		Status:  CASPatchStatusFailed,
		PatchID: strings.TrimSpace(input.PatchID),
		Paths:   make([]CASPatchPathResult, 0),
		Issues:  make([]CASPatchIssue, 0),
	}

	authority := input.Context.WithDefaults()
	if err := authority.Validate(); err != nil {
		result.Issues = append(result.Issues, casPatchFailure("", CASPatchIssueContextInvalid, fmt.Sprintf("repo authority context is invalid: %v", err)))
	} else {
		contextDigest, err := authority.Digest()
		if err != nil {
			result.Issues = append(result.Issues, casPatchFailure("", CASPatchIssueContextInvalid, fmt.Sprintf("repo authority context digest failed: %v", err)))
		} else {
			result.ContextDigest = contextDigest
		}
	}

	candidates := normalizeCASPatchCandidates(input.CandidateFileHashes, &result)
	if len(candidates) > 0 {
		result.PatchDigest = digestCASPatchCandidates(candidates)
	}
	if len(input.CandidateFileHashes) == 0 {
		result.Issues = append(result.Issues, casPatchFailure("", CASPatchIssueCandidateHashesRequired, "candidate_file_hashes are required"))
		return finalizeCASPatchResult(result)
	}

	for _, candidate := range candidates {
		pathResult := CASPatchPathResult{
			Path:          candidate.path,
			Status:        CASPatchStatusApplied,
			BaseHash:      strings.TrimSpace(authority.Base.FileHashes[candidate.path]),
			CurrentHash:   strings.TrimSpace(input.CurrentFileHashes[candidate.path]),
			CandidateHash: candidate.hash,
		}
		if pathResult.BaseHash == "" && pathResult.CurrentHash == "" {
			pathResult.ChangeKind = CASPatchChangeAdd
		}

		if !pathsetCoversPath(authority.Pathset, candidate.path) {
			pathResult.Status = CASPatchStatusFailed
			result.Issues = append(result.Issues, casPatchFailure(candidate.path, CASPatchIssuePathOutsideContext, fmt.Sprintf("candidate path %q is outside repo authority context pathset", candidate.path)))
			result.Paths = append(result.Paths, pathResult)
			continue
		}
		if casPatchPathChangeKind(pathResult) == CASPatchChangeAdd {
			result.Paths = append(result.Paths, pathResult)
			continue
		}
		if pathResult.BaseHash == "" {
			pathResult.Status = CASPatchStatusFailed
			result.Issues = append(result.Issues, casPatchFailure(candidate.path, CASPatchIssueBaseHashMissing, fmt.Sprintf("base hash is missing for %q", candidate.path)))
			result.Paths = append(result.Paths, pathResult)
			continue
		}
		if pathResult.CurrentHash == "" {
			pathResult.Status = CASPatchStatusFailed
			result.Issues = append(result.Issues, casPatchFailure(candidate.path, CASPatchIssueCurrentHashMissing, fmt.Sprintf("current hash is missing for %q", candidate.path)))
			result.Paths = append(result.Paths, pathResult)
			continue
		}
		if pathResult.CurrentHash != pathResult.BaseHash {
			pathResult.Status = CASPatchStatusConflict
			result.Issues = append(result.Issues, CASPatchIssue{
				Status:        CASPatchStatusConflict,
				Kind:          CASPatchIssueBaseDrift,
				Path:          candidate.path,
				Message:       fmt.Sprintf("base drift for %q", candidate.path),
				ExpectedHash:  pathResult.BaseHash,
				ActualHash:    pathResult.CurrentHash,
				CandidateHash: pathResult.CandidateHash,
			})
			result.Paths = append(result.Paths, pathResult)
			continue
		}
		result.Paths = append(result.Paths, pathResult)
	}

	return finalizeCASPatchResult(result)
}

func normalizeCASPatchCandidates(raw map[string]string, result *CASPatchApplyResult) []casPatchCandidateEntry {
	candidates := make([]casPatchCandidateEntry, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	rawPaths := make([]string, 0, len(raw))
	for rawPath := range raw {
		rawPaths = append(rawPaths, rawPath)
	}
	sort.Strings(rawPaths)
	for _, rawPath := range rawPaths {
		normalized, err := normalizeResolverPath(rawPath)
		if err != nil {
			result.Issues = append(result.Issues, casPatchFailure(rawPath, CASPatchIssueCandidatePathInvalid, err.Error()))
			continue
		}
		if normalized != rawPath {
			result.Issues = append(result.Issues, casPatchFailure(rawPath, CASPatchIssueCandidatePathUnstable, fmt.Sprintf("candidate path is not normalized: got %q want %q", rawPath, normalized)))
			continue
		}
		if _, ok := seen[normalized]; ok {
			result.Issues = append(result.Issues, casPatchFailure(normalized, CASPatchIssueCandidatePathDuplicate, fmt.Sprintf("candidate path %q is duplicated after normalization", normalized)))
			continue
		}
		hash := strings.TrimSpace(raw[rawPath])
		if hash == "" {
			result.Issues = append(result.Issues, casPatchFailure(normalized, CASPatchIssueCandidateHashMissing, fmt.Sprintf("candidate hash is missing for %q", normalized)))
			continue
		}
		seen[normalized] = struct{}{}
		candidates = append(candidates, casPatchCandidateEntry{path: normalized, hash: hash})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].path < candidates[j].path
	})
	return candidates
}

func finalizeCASPatchResult(result CASPatchApplyResult) CASPatchApplyResult {
	sort.Slice(result.Paths, func(i, j int) bool {
		return result.Paths[i].Path < result.Paths[j].Path
	})
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Status != result.Issues[j].Status {
			return result.Issues[i].Status < result.Issues[j].Status
		}
		if result.Issues[i].Path != result.Issues[j].Path {
			return result.Issues[i].Path < result.Issues[j].Path
		}
		return result.Issues[i].Kind < result.Issues[j].Kind
	})
	hasFailure := false
	hasConflict := false
	for _, issue := range result.Issues {
		switch issue.Status {
		case CASPatchStatusFailed:
			hasFailure = true
		case CASPatchStatusConflict:
			hasConflict = true
		}
	}
	switch {
	case hasFailure:
		result.Status = CASPatchStatusFailed
	case hasConflict:
		result.Status = CASPatchStatusConflict
	default:
		result.Status = CASPatchStatusApplied
	}
	if len(result.Paths) == 0 {
		result.Paths = nil
	}
	if len(result.Issues) == 0 {
		result.Issues = nil
	}
	return result
}

func casPatchPathChangeKind(pathResult CASPatchPathResult) string {
	changeKind := strings.TrimSpace(pathResult.ChangeKind)
	if changeKind == "" {
		return CASPatchChangeModify
	}
	return changeKind
}

func casPatchFailure(path, kind, message string) CASPatchIssue {
	return CASPatchIssue{
		Status:  CASPatchStatusFailed,
		Kind:    kind,
		Path:    path,
		Message: message,
	}
}

func digestCASPatchCandidates(candidates []casPatchCandidateEntry) string {
	var b strings.Builder
	b.WriteString(CASPatchApplySchemaVersion)
	for _, candidate := range candidates {
		b.WriteByte(0)
		b.WriteString(candidate.path)
		b.WriteByte(0)
		b.WriteString(candidate.hash)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}
