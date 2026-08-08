package repoauthority

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type PathResolver struct {
	repoRoot string
}

type ResolvedPath struct {
	RepoRoot         string `json:"repo_root"`
	RelPath          string `json:"rel_path"`
	AbsPath          string `json:"abs_path"`
	Existing         bool   `json:"existing"`
	ExistingAncestor string `json:"existing_ancestor,omitempty"`
}

func NewPathResolver(repoRoot string) (PathResolver, error) {
	root := strings.TrimSpace(repoRoot)
	if root == "" {
		return PathResolver{}, fmt.Errorf("repo_root is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return PathResolver{}, fmt.Errorf("resolve repo root: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return PathResolver{}, fmt.Errorf("canonicalize repo root: %w", err)
	}
	info, err := os.Stat(canonicalRoot)
	if err != nil {
		return PathResolver{}, fmt.Errorf("stat repo root: %w", err)
	}
	if !info.IsDir() {
		return PathResolver{}, fmt.Errorf("repo_root is not a directory: %s", repoRoot)
	}
	return PathResolver{repoRoot: filepath.Clean(canonicalRoot)}, nil
}

func (r PathResolver) RepoRoot() string {
	return r.repoRoot
}

func (r PathResolver) Resolve(rawPath string) (ResolvedPath, error) {
	if strings.TrimSpace(r.repoRoot) == "" {
		return ResolvedPath{}, fmt.Errorf("path resolver repo_root is required")
	}
	relPath, err := normalizeResolverPath(rawPath)
	if err != nil {
		return ResolvedPath{}, err
	}
	absPath := filepath.Clean(filepath.Join(r.repoRoot, filepath.FromSlash(relPath)))
	if err := ensurePathInsideRoot(r.repoRoot, absPath); err != nil {
		return ResolvedPath{}, err
	}
	existing, ancestor, err := r.verifyExistingPathChain(relPath)
	if err != nil {
		return ResolvedPath{}, err
	}
	return ResolvedPath{
		RepoRoot:         r.repoRoot,
		RelPath:          relPath,
		AbsPath:          absPath,
		Existing:         existing,
		ExistingAncestor: ancestor,
	}, nil
}

func (r PathResolver) ResolvePathSet(paths []string) ([]ResolvedPath, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("pathset is required")
	}
	resolved := make([]ResolvedPath, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	previous := ""
	for i, rawPath := range paths {
		item, err := r.Resolve(rawPath)
		if err != nil {
			return nil, fmt.Errorf("pathset[%d]: %w", i, err)
		}
		if rawPath != item.RelPath {
			return nil, fmt.Errorf("pathset[%d] is not normalized: got %q want %q", i, rawPath, item.RelPath)
		}
		if _, ok := seen[item.RelPath]; ok {
			return nil, fmt.Errorf("pathset contains duplicate path %q", item.RelPath)
		}
		if i > 0 && item.RelPath < previous {
			return nil, fmt.Errorf("pathset is not sorted at index %d: %q before %q", i, item.RelPath, previous)
		}
		seen[item.RelPath] = struct{}{}
		previous = item.RelPath
		resolved = append(resolved, item)
	}
	return resolved, nil
}

func normalizeResolverPath(raw string) (string, error) {
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
	for _, part := range strings.Split(slashPath, "/") {
		if part == ".." {
			return "", fmt.Errorf("path traversal is not allowed: %q", raw)
		}
	}
	cleaned := path.Clean(slashPath)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("path is required")
	}
	return cleaned, nil
}

func (r PathResolver) verifyExistingPathChain(relPath string) (bool, string, error) {
	current := r.repoRoot
	ancestor := r.repoRoot
	parts := strings.Split(relPath, "/")
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return false, ancestor, nil
		}
		if err != nil {
			return false, ancestor, fmt.Errorf("inspect path %q: %w", relPath, err)
		}
		if err := rejectLinkOrEscapedResolvedPath(r.repoRoot, current, info); err != nil {
			return false, ancestor, err
		}
		if info.IsDir() {
			if err := rejectNestedGitBoundary(r.repoRoot, current, relPath); err != nil {
				return false, ancestor, err
			}
		} else if i < len(parts)-1 {
			return false, ancestor, fmt.Errorf("path component is not a directory: %q", strings.Join(parts[:i+1], "/"))
		}
		ancestor = current
	}
	return true, ancestor, nil
}

func rejectLinkOrEscapedResolvedPath(repoRoot, current string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlink path component is not allowed: %s", current)
	}
	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return fmt.Errorf("canonicalize path component %q: %w", current, err)
	}
	if err := ensurePathInsideRoot(repoRoot, resolved); err != nil {
		return fmt.Errorf("resolved path escapes repo root: %w", err)
	}
	return nil
}

func rejectNestedGitBoundary(repoRoot, dir, relPath string) error {
	if samePath(repoRoot, dir) {
		return nil
	}
	gitMarker := filepath.Join(dir, ".git")
	if _, err := os.Lstat(gitMarker); err == nil {
		return fmt.Errorf("path crosses nested git repository boundary while resolving %q: %s", relPath, dir)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect nested git boundary %q: %w", gitMarker, err)
	}
	return nil
}

func ensurePathInsideRoot(repoRoot, candidate string) error {
	root := filepath.Clean(repoRoot)
	cleanCandidate := filepath.Clean(candidate)
	rel, err := filepath.Rel(root, cleanCandidate)
	if err != nil {
		return fmt.Errorf("compute repo-relative path for %q: %w", candidate, err)
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("path escapes repo root: %s", candidate)
	}
	return nil
}

func samePath(a, b string) bool {
	rel, err := filepath.Rel(filepath.Clean(a), filepath.Clean(b))
	return err == nil && rel == "."
}
