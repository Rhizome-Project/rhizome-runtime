package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

func projectPatchQueueResolveRepoRoot(localPath string) (string, error) {
	root, err := filepath.Abs(strings.TrimSpace(localPath))
	if err != nil || strings.TrimSpace(localPath) == "" {
		return "", fmt.Errorf("local_path is required and must be absolute/resolvable")
	}
	if info, err := os.Stat(root); err != nil {
		return "", fmt.Errorf("local_path %s is not readable: %w", root, err)
	} else if !info.IsDir() {
		return "", fmt.Errorf("local_path %s is not a directory", root)
	}
	if evalRoot, err := filepath.EvalSymlinks(root); err == nil {
		root = evalRoot
	}
	return root, nil
}

func projectPatchQueueConcreteCandidatePaths(ctx context.Context, repoRoot, baseSHA, headSHA string, pathset, explicit []string) ([]string, error) {
	baseSHA = strings.TrimSpace(baseSHA)
	headSHA = strings.TrimSpace(headSHA)
	if baseSHA != "" && headSHA != "" {
		unsupportedPaths, err := projectPatchQueueUnsupportedGitDiffPaths(ctx, repoRoot, baseSHA, headSHA, pathset)
		if err != nil {
			return nil, fmt.Errorf("derive unsupported candidate_paths from git diff %s..%s: %w", baseSHA, headSHA, err)
		}
		if len(unsupportedPaths) > 0 {
			return nil, fmt.Errorf("controlled patch queue deletions are not supported yet; renames are not supported yet either; remove deleted or renamed paths or use a future deletion lane: %s", strings.Join(unsupportedPaths, ", "))
		}
	}
	if len(explicit) > 0 {
		return projectPatchQueueNormalizeCandidatePaths(explicit, pathset)
	}
	if !projectPatchQueuePathsetHasScoped(pathset) {
		return projectPatchQueueNormalizeCandidatePaths(pathset, pathset)
	}
	if baseSHA == "" || headSHA == "" {
		return nil, fmt.Errorf("candidate_paths are required for scoped pathsets when base_sha/head_sha are unavailable")
	}
	paths, err := projectPatchQueueCoveredGitDiffPaths(ctx, repoRoot, baseSHA, headSHA, pathset, "ACMT")
	if err != nil {
		return nil, fmt.Errorf("derive candidate_paths from git diff %s..%s: %w", baseSHA, headSHA, err)
	}
	return projectPatchQueueNormalizeCandidatePaths(paths, pathset)
}

func projectPatchQueueUnsupportedGitDiffPaths(ctx context.Context, repoRoot, baseSHA, headSHA string, pathset []string) ([]string, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "diff", "--name-status", "--find-renames", "--diff-filter=DR", baseSHA+".."+headSHA).Output()
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(out), "\n")
	unsupported := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "\t")
		if len(fields) < 2 {
			continue
		}
		status := strings.TrimSpace(fields[0])
		switch {
		case status == "D":
			path, err := normalizeReviewPath(fields[1])
			if err != nil {
				return nil, fmt.Errorf("deleted candidate path %q is invalid: %w", fields[1], err)
			}
			if pathsetIsCoveredByScope([]string{path}, pathset) {
				unsupported = append(unsupported, path)
			}
		case strings.HasPrefix(status, "R"):
			if len(fields) < 3 {
				return nil, fmt.Errorf("rename candidate entry is malformed: %q", line)
			}
			oldPath, err := normalizeReviewPath(fields[1])
			if err != nil {
				return nil, fmt.Errorf("rename source path %q is invalid: %w", fields[1], err)
			}
			newPath, err := normalizeReviewPath(fields[2])
			if err != nil {
				return nil, fmt.Errorf("rename destination path %q is invalid: %w", fields[2], err)
			}
			if pathsetIsCoveredByScope([]string{oldPath}, pathset) || pathsetIsCoveredByScope([]string{newPath}, pathset) {
				unsupported = append(unsupported, oldPath+" -> "+newPath)
			}
		}
	}
	sort.Strings(unsupported)
	return unsupported, nil
}

func projectPatchQueueCoveredGitDiffPaths(ctx context.Context, repoRoot, baseSHA, headSHA string, pathset []string, diffFilter string) ([]string, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "diff", "--name-only", "--diff-filter="+diffFilter, baseSHA+".."+headSHA).Output()
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(out), "\n")
	paths := make([]string, 0, len(lines))
	for _, line := range lines {
		pathValue := strings.TrimSpace(line)
		if pathValue == "" {
			continue
		}
		normalized, err := normalizeReviewPath(pathValue)
		if err != nil {
			return nil, fmt.Errorf("candidate path %q is invalid: %w", pathValue, err)
		}
		if pathsetIsCoveredByScope([]string{normalized}, pathset) {
			paths = append(paths, normalized)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func projectPatchQueueNormalizeCandidatePaths(rawPaths, pathset []string) ([]string, error) {
	seen := map[string]struct{}{}
	paths := make([]string, 0, len(rawPaths))
	for _, raw := range rawPaths {
		normalized, err := normalizeReviewPath(raw)
		if err != nil {
			return nil, fmt.Errorf("candidate path %q is invalid: %w", raw, err)
		}
		if !pathsetIsCoveredByScope([]string{normalized}, pathset) {
			return nil, fmt.Errorf("candidate path %s is outside patch queue pathset", normalized)
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("at least one concrete candidate path is required")
	}
	return paths, nil
}

func projectPatchQueuePathsetHasScoped(pathset []string) bool {
	for _, item := range pathset {
		if scopePathIsScoped(item) {
			return true
		}
	}
	return false
}

func projectPatchQueueBaseFileHashes(ctx context.Context, repoRoot, baseSHA, headSHA string, pathset, candidatePaths []string, rawJSON string) (map[string]string, error) {
	hashes, err := projectPatchQueueParseFileHashesJSON(rawJSON, pathset)
	if err != nil {
		return nil, err
	}
	if hashes == nil {
		hashes = map[string]string{}
	}
	baseSHA = strings.TrimSpace(baseSHA)
	if baseSHA == "" {
		return nil, fmt.Errorf("base_sha is required to derive base_file_hashes")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return nil, fmt.Errorf("head_sha is required to derive base_file_hashes")
	}
	for _, candidatePath := range candidatePaths {
		headExists, err := projectPatchQueueGitObjectExists(ctx, repoRoot, headSHA, candidatePath)
		if err != nil {
			return nil, fmt.Errorf("candidate head path %s: %w", candidatePath, err)
		}
		if !headExists {
			return nil, fmt.Errorf("controlled patch queue deletions are not supported yet; candidate path %s is absent at head %s", candidatePath, headSHA)
		}
		supplied := strings.TrimSpace(hashes[candidatePath])
		baseExists, err := projectPatchQueueGitObjectExists(ctx, repoRoot, baseSHA, candidatePath)
		if err != nil {
			return nil, fmt.Errorf("base_file_hashes[%s]: %w", candidatePath, err)
		}
		if !baseExists {
			if supplied != "" {
				return nil, fmt.Errorf("base_file_hashes[%s] was supplied for an added path; additions must not carry base hashes", candidatePath)
			}
			continue
		}
		digest, err := projectPatchQueueGitObjectContentDigest(ctx, repoRoot, baseSHA, candidatePath)
		if err != nil {
			return nil, fmt.Errorf("base_file_hashes[%s]: %w", candidatePath, err)
		}
		if supplied != "" && supplied != digest {
			return nil, fmt.Errorf("base_file_hashes[%s] mismatch: got %s want %s", candidatePath, supplied, digest)
		}
		hashes[candidatePath] = digest
	}
	for _, candidatePath := range candidatePaths {
		if strings.TrimSpace(hashes[candidatePath]) == "" {
			baseExists, err := projectPatchQueueGitObjectExists(ctx, repoRoot, baseSHA, candidatePath)
			if err != nil {
				return nil, fmt.Errorf("base_file_hashes[%s]: %w", candidatePath, err)
			}
			if baseExists {
				return nil, fmt.Errorf("base_file_hashes[%s] is required", candidatePath)
			}
		}
	}
	return hashes, nil
}

func projectPatchQueueGitObjectExists(ctx context.Context, repoRoot, ref, normalizedPath string) (bool, error) {
	normalizedPath, err := normalizeReviewPath(normalizedPath)
	if err != nil {
		return false, err
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false, fmt.Errorf("git ref is required")
	}
	if out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "rev-parse", "--verify", "--quiet", ref+"^{tree}").CombinedOutput(); err != nil {
		return false, fmt.Errorf("git ref %s is not a valid tree-ish: %s: %w", ref, strings.TrimSpace(string(out)), err)
	}
	err = exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "cat-file", "-e", ref+":"+normalizedPath).Run()
	return err == nil, nil
}

func projectPatchQueueParseFileHashesJSON(rawJSON string, pathset []string) (map[string]string, error) {
	rawJSON = strings.TrimSpace(rawJSON)
	if rawJSON == "" {
		return nil, nil
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(rawJSON), &payload); err != nil {
		return nil, fmt.Errorf("base_file_hashes_json must be an object: %w", err)
	}
	hashes := make(map[string]string, len(payload))
	for rawPath, rawHash := range payload {
		normalized, err := normalizeReviewPath(rawPath)
		if err != nil {
			return nil, fmt.Errorf("base_file_hashes_json path %q is invalid: %w", rawPath, err)
		}
		if normalized != rawPath {
			return nil, fmt.Errorf("base_file_hashes_json path %q must be normalized as %q", rawPath, normalized)
		}
		if !pathsetIsCoveredByScope([]string{normalized}, pathset) {
			return nil, fmt.Errorf("base_file_hashes_json path %s is outside patch queue pathset", normalized)
		}
		hash := strings.TrimSpace(rawHash)
		if hash == "" {
			return nil, fmt.Errorf("base_file_hashes_json path %s has empty digest", normalized)
		}
		hashes[normalized] = hash
	}
	return hashes, nil
}

func projectPatchQueueGitObjectContentDigest(ctx context.Context, repoRoot, ref, normalizedPath string) (string, error) {
	normalizedPath, err := normalizeReviewPath(normalizedPath)
	if err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "show", strings.TrimSpace(ref)+":"+normalizedPath).Output()
	if err != nil {
		return "", fmt.Errorf("git show %s:%s failed: %w", strings.TrimSpace(ref), normalizedPath, err)
	}
	if int64(len(out)) > projectPatchQueueMaterializationMaxFileBytes {
		return "", fmt.Errorf("git object %s exceeds file size limit %d bytes", normalizedPath, projectPatchQueueMaterializationMaxFileBytes)
	}
	if !utf8.Valid(out) {
		return "", fmt.Errorf("git object %s is not valid UTF-8", normalizedPath)
	}
	return projectPatchQueueMaterializationContentDigest(string(out)), nil
}

func projectPatchQueueGitTreeHash(ctx context.Context, repoRoot, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("base_sha is required to derive base_tree_hash")
	}
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "rev-parse", ref+"^{tree}").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse %s^{tree} failed: %w", ref, err)
	}
	treeHash := strings.TrimSpace(string(out))
	if treeHash == "" {
		return "", fmt.Errorf("git tree hash for %s is empty", ref)
	}
	return treeHash, nil
}

func projectPatchQueueGeneratedRepoLeaseID(workspaceID, projectID, queueID, itemID, agentID, runID string) string {
	seed := strings.Join([]string{
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(queueID),
		strings.TrimSpace(itemID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(runID),
	}, "\x00")
	prefix := sanitizeRefSegment(strings.Join([]string{
		"lease",
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(queueID),
		strings.TrimSpace(itemID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(runID),
	}, "-"))
	if len(prefix) > 68 {
		prefix = strings.Trim(prefix[:68], "-.")
	}
	if prefix == "" {
		prefix = "lease"
	}
	return prefix + "-" + shortRefHash(seed)
}

func projectPatchQueueVerifyGitCheckoutReady(ctx context.Context, repoRoot string, item ProjectPatchQueueItemRecord, candidatePaths []string) error {
	if strings.TrimSpace(repoRoot) == "" {
		return fmt.Errorf("local_path is required")
	}
	headSHA := strings.TrimSpace(item.HeadSHA)
	if headSHA == "" {
		return fmt.Errorf("patch queue item head_sha is required")
	}
	currentHead, err := gitRevParse(ctx, repoRoot, "HEAD")
	if err != nil {
		return fmt.Errorf("read checkout HEAD: %w", err)
	}
	if currentHead != headSHA {
		return fmt.Errorf("checkout HEAD %s does not match patch queue head_sha %s", currentHead, headSHA)
	}
	args := []string{"-C", repoRoot, "status", "--porcelain", "--"}
	for _, candidatePath := range candidatePaths {
		normalized, err := normalizeReviewPath(candidatePath)
		if err != nil {
			return err
		}
		args = append(args, normalized)
	}
	out, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		return fmt.Errorf("check candidate path cleanliness: %w", err)
	}
	if strings.TrimSpace(string(out)) != "" {
		return fmt.Errorf("candidate paths must be clean before CAS/materialization; dirty status: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

func projectPatchQueueGitWorktreeAvailable(ctx context.Context, repoRoot string) bool {
	out, err := exec.CommandContext(ctx, "git", "-C", strings.TrimSpace(repoRoot), "rev-parse", "--is-inside-work-tree").Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}
