package sqlite

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// Ground-truth git read for the NO-VENDORING gate. Reads go.mod at a candidate commit from
// the local managed-remote so the accept gate verifies actual code, not a reviewer's
// attestation. Callers MUST invoke this BEFORE opening the accept write-transaction - never
// hold the SQLite write lock across git process I/O. Reading an immutable object at a fixed
// SHA is race-safe versus a concurrent mutation-actuator push.

// localGitDirFromRemoteURL resolves a project repository RemoteURL to a local git directory.
// Only local managed-remotes (file:// or a bare local path) are readable for the ground-truth
// check; a non-local remote returns ok=false so the caller fails closed.
func localGitDirFromRemoteURL(remoteURL string) (string, bool) {
	u := strings.TrimSpace(remoteURL)
	if u == "" {
		return "", false
	}
	if !strings.HasPrefix(u, "file://") {
		if strings.Contains(u, "://") {
			return "", false // remote (https/ssh/...) - not locally readable
		}
		return filepath.FromSlash(u), true // bare local path
	}
	p := strings.TrimPrefix(u, "file://")
	if strings.TrimSpace(p) == "" {
		return "", false
	}
	// file:///C:/path -> /C:/path on Windows; strip the leading slash before a drive letter.
	if len(p) >= 3 && p[0] == '/' && p[2] == ':' {
		p = p[1:]
	}
	localPath := filepath.Clean(filepath.FromSlash(p))
	if localPath == "." {
		return "", false
	}
	return localPath, true
}

func localGitDirCandidatesFromRemoteURL(remoteURL string) ([]string, bool) {
	direct, ok := localGitDirFromRemoteURL(remoteURL)
	if !ok {
		return nil, false
	}
	return []string{direct}, true
}

// readGoModAtGitHead returns the go.mod blob at headSHA from a (bare) git directory.
func readGoModAtGitHead(ctx context.Context, gitDir, headSHA string) (string, error) {
	gitDir = strings.TrimSpace(gitDir)
	headSHA = strings.TrimSpace(headSHA)
	if gitDir == "" || headSHA == "" {
		return "", fmt.Errorf("git dir and head sha are required")
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "show", headSHA+":go.mod")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git show %s:go.mod from %s: %w", headSHA, gitDir, err)
	}
	return string(out), nil
}

func readGoModAtGitHeadFromCandidates(ctx context.Context, gitDirs []string, headSHA string) (string, error) {
	var failures []string
	for _, gitDir := range gitDirs {
		goMod, err := readGoModAtGitHead(ctx, gitDir, headSHA)
		if err == nil {
			return goMod, nil
		}
		failures = append(failures, err.Error())
	}
	return "", fmt.Errorf("all managed-remote candidates failed: %s", strings.Join(failures, "; "))
}

func readGoModAtRemoteURLHead(ctx context.Context, remoteURL, headSHA string) (string, error) {
	gitDirs, ok := localGitDirCandidatesFromRemoteURL(remoteURL)
	if !ok {
		return "", fmt.Errorf("project repository is not a local managed-remote")
	}
	return readGoModAtGitHeadFromCandidates(ctx, gitDirs, headSHA)
}

// projectInterpreterVendoringVerdict reads go.mod at headSHA from the local managed-remote
// (resolved from the project repository RemoteURL) and returns blocking reasons. Fail-closed:
// a non-local remote, an unreadable repo, or a read error yields a blocking reason so the gate
// never passes on absence of proof.
func projectInterpreterVendoringVerdict(ctx context.Context, remoteURL, headSHA string) []string {
	if strings.TrimSpace(headSHA) == "" {
		return []string{"no_vendoring_unproven: candidate head sha is empty"}
	}
	goMod, err := readGoModAtRemoteURLHead(ctx, remoteURL, headSHA)
	if err != nil {
		return []string{"no_vendoring_unproven: cannot read go.mod at head: " + err.Error()}
	}
	return interpreterVendoringViolations(goMod)
}
