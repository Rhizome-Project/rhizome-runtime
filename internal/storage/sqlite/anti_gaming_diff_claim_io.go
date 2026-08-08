package sqlite

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// Ground-truth diff read for the diff-implements-claim gate. Returns the added-line count per
// changed file for base..head from a local managed-remote, so the gate verifies a claimed
// capability against the ACTUAL diff. Like the NO-VENDORING read, callers MUST invoke this
// BEFORE the accept write-transaction (never exec git under the SQLite write lock).
func readDiffNumstatAtGitHead(ctx context.Context, gitDir, baseSHA, headSHA string) (map[string]int, error) {
	gitDir = strings.TrimSpace(gitDir)
	baseSHA = strings.TrimSpace(baseSHA)
	headSHA = strings.TrimSpace(headSHA)
	if gitDir == "" || headSHA == "" {
		return nil, fmt.Errorf("git dir and head sha are required")
	}
	rangeArg := headSHA
	if baseSHA != "" {
		rangeArg = baseSHA + ".." + headSHA
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "diff", "--numstat", rangeArg)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git diff --numstat %s from %s: %w", rangeArg, gitDir, err)
	}
	added := map[string]int{}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		n, perr := strconv.Atoi(fields[0]) // "-" for binary files -> skip
		if perr != nil {
			continue
		}
		path := fields[len(fields)-1] // last field is the (new) path; rename arrows are earlier
		added[path] += n
	}
	return added, nil
}

func readDiffNumstatAtRemoteURLHead(ctx context.Context, remoteURL, baseSHA, headSHA string) (map[string]int, error) {
	gitDirs, ok := localGitDirCandidatesFromRemoteURL(remoteURL)
	if !ok {
		return nil, fmt.Errorf("project repository is not a local managed-remote")
	}
	var failures []string
	for _, gitDir := range gitDirs {
		added, err := readDiffNumstatAtGitHead(ctx, gitDir, baseSHA, headSHA)
		if err == nil {
			return added, nil
		}
		failures = append(failures, err.Error())
	}
	return nil, fmt.Errorf("all managed-remote candidates failed: %s", strings.Join(failures, "; "))
}

// projectDiffClaimVerdict reads the base..head numstat from the local managed-remote and, given
// a parsed capability claim, returns blocking reasons. Fail-closed: a present claim whose diff
// cannot be read yields a blocking reason. No claim => nil (nothing to verify).
func projectDiffClaimVerdict(ctx context.Context, remoteURL, baseSHA, headSHA string, claim capabilityClaim) []string {
	if !claim.Present {
		return nil
	}
	if strings.TrimSpace(headSHA) == "" {
		return []string{"claim_unverifiable: candidate head sha is empty"}
	}
	added, err := readDiffNumstatAtRemoteURLHead(ctx, remoteURL, baseSHA, headSHA)
	if err != nil {
		return []string{"claim_unverifiable: cannot read base..head diff: " + err.Error()}
	}
	return diffImplementsClaimViolations(claim, added)
}
