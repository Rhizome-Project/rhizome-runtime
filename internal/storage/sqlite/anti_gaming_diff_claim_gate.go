package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// diff-implements-claim accept-gate wiring. The base..head diff numstat is read BEFORE the
// accept write-transaction (git off the SQLite write lock); the structured capability claim is
// parsed from the decision text in-tx and verified against that diff. Scoped to the same opt-in
// as NO-VENDORING (the interpreter campaign opts into the whole anti-gaming gate).

type diffClaimVerdict struct {
	Required bool
	HeadSHA  string
	AddedLOC map[string]int
}

// computeDiffClaimDiffStats loads the candidate item + repo + scoping config in a read-only tx,
// then (only if scoped-required) reads the base..head numstat from the managed-remote. Call this
// BEFORE the accept write-transaction. Fail-closed: when required, an unreadable diff leaves
// AddedLOC nil so any present claim is treated as unbacked and blocks in-tx.
func (s *Store) computeDiffClaimDiffStats(ctx context.Context, workspaceID, projectID, queueID, itemID string) (diffClaimVerdict, error) {
	var (
		required  bool
		baseSHA   string
		headSHA   string
		remoteURL string
		loadErr   error
	)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return diffClaimVerdict{}, err
	}
	func() {
		defer func() { _ = tx.Rollback() }()
		cfgKey := projectNoVendoringConfigDocKey(projectID)
		if strings.TrimSpace(cfgKey) == "" {
			return
		}
		if _, derr := s.loadWorkspaceDocTx(ctx, tx, workspaceID, cfgKey); derr != nil {
			if errors.Is(derr, sql.ErrNoRows) {
				return
			}
			loadErr = derr
			return
		}
		required = true
		item, ok, ierr := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if ierr != nil {
			loadErr = ierr
			return
		}
		if !ok {
			return
		}
		baseSHA = strings.TrimSpace(item.BaseSHA)
		headSHA = strings.TrimSpace(item.HeadSHA)
		repo, rerr := getProjectRepositoryTx(ctx, tx, workspaceID, projectID, item.RepoID)
		if rerr == nil {
			remoteURL = strings.TrimSpace(repo.RemoteURL)
		}
	}()
	if loadErr != nil {
		return diffClaimVerdict{}, loadErr
	}
	if !required {
		return diffClaimVerdict{Required: false}, nil
	}
	verdict := diffClaimVerdict{Required: true, HeadSHA: headSHA}
	if headSHA != "" {
		if added, derr := readDiffNumstatAtRemoteURLHead(ctx, remoteURL, baseSHA, headSHA); derr == nil {
			verdict.AddedLOC = added
		}
		// On read error AddedLOC stays nil -> a present claim is unbacked -> blocks in-tx (fail-closed).
	}
	return verdict, nil
}
